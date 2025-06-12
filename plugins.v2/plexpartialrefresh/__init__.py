import time
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional
import requests
import xml.etree.ElementTree as ET
import threading
from collections import defaultdict

from app.core.context import MediaInfo
from app.core.event import eventmanager, Event
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import TransferInfo, RefreshMediaItem, ServiceInfo
from app.schemas.types import EventType

class PlexPartialRefresh(_PluginBase):
    plugin_name = "Plex局部刷新"
    plugin_desc = "整理（硬链接）成功后，自动通知Plex服务器对新资源进行局部刷新，支持路径映射。"
    plugin_icon = "sync.png"
    plugin_version = "1.0.1"
    plugin_author = "HisingX"
    author_url = "https://github.com/HisingX"
    plugin_config_prefix = "plexpartialrefresh_"
    plugin_order = 15
    auth_level = 1

    mediaserver_helper = None
    _enabled = False
    _plex_server = None
    _path_maps = []
    _batch_delay = 60  # 批量延迟时间（秒）
    
    # 批量刷新相关
    _pending_refreshes = defaultdict(set)  # {parent_path: {child_paths}}
    _refresh_timers = {}  # {parent_path: timer}
    _timer_lock = threading.Lock()

    def init_plugin(self, config: dict = None):
        self.mediaserver_helper = MediaServerHelper()
        # 支持兼容文本映射配置
        if config:
            self._enabled = config.get("enabled", False)
            self._plex_server = config.get("plex_server")
            # 确保 batch_delay 是整数类型
            try:
                self._batch_delay = int(config.get("batch_delay", 60))
                # 限制在合理范围内
                if self._batch_delay < 10:
                    self._batch_delay = 10
                elif self._batch_delay > 300:
                    self._batch_delay = 300
            except (ValueError, TypeError):
                logger.warning("批量延迟时间配置无效，使用默认值60秒")
                self._batch_delay = 60
            
            # 支持两种配置来源
            self._path_maps = config.get("path_maps", [])
            path_maps_text = config.get("path_maps_text", "")
            if path_maps_text:
                self._path_maps = []
                for line in path_maps_text.splitlines():
                    if "=>" in line:
                        local, plex = line.split("=>", 1)
                        self._path_maps.append({
                            'local': local.strip(),
                            'plex': plex.strip()
                        })

    @property
    def plex_service(self) -> Optional[ServiceInfo]:
        if not self._plex_server:
            logger.warning("未选择Plex服务器，请检查插件配置")
            return None
        services = self.mediaserver_helper.get_services(name_filters=[self._plex_server])
        if not services or self._plex_server not in services:
            logger.warning("获取Plex服务器实例失败，请检查配置")
            return None
        service_info = services[self._plex_server]
        if service_info.instance.is_inactive():
            logger.warning(f"Plex服务器 {self._plex_server} 未连接，请检查配置")
            return None
        return service_info

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        配置页面：启用、Plex服务器选择、路径映射表、批量延迟时间
        """
        plex_servers = [
            {"title": config.name, "value": config.name}
            for config in self.mediaserver_helper.get_configs().values()
            if config.type == "plex"
        ]
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 12},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'plex_server',
                                            'label': 'Plex服务器',
                                            'items': plex_servers,
                                            'clearable': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'batch_delay',
                                            'label': '批量延迟时间（秒）',
                                            'type': 'number',
                                            'min': 10,
                                            'max': 300,
                                            'hint': '同一剧集多集资源的合并刷新延迟时间'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'path_maps_text',
                                            'label': '路径映射（每行一组，本地前缀=>Plex前缀）',
                                            'placeholder': '/files1 => E:/videos \n/files2 => E:/movies',
                                        }
                                    }
                                ]
                            }
                        ]
                    }                 
                ]
            }
        ], {
            "enabled": False,
            "plex_server": None,
            "path_maps": [],
            "path_maps_text": "",
            "batch_delay": 60
        }

    def get_page(self) -> List[dict]:
        pass

    def map_path(self, local_path: str) -> Optional[str]:
        """
        将本地路径映射为Plex服务器路径，采用最长前缀优先匹配
        """
        best_match = None
        best_prefix_len = -1
        for mapping in self._path_maps:
            local_prefix = mapping.get('local')
            plex_prefix = mapping.get('plex')
            if local_prefix and plex_prefix and local_path.startswith(local_prefix):
                if len(local_prefix) > best_prefix_len:
                    best_match = (local_prefix, plex_prefix)
                    best_prefix_len = len(local_prefix)
        if best_match:
            local_prefix, plex_prefix = best_match
            mapped = local_path.replace(local_prefix, plex_prefix, 1)
            logger.info(f"路径映射: {local_path} => {mapped}")
            return mapped
        logger.warning(f"未找到路径映射: {local_path}")
        return None

    def _get_plex_connection_info(self) -> Optional[Dict[str, str]]:
        """
        获取Plex服务器连接信息（URL和Token）
        """
        if not self.plex_service:
            return None
        
        try:
            # 从服务配置中获取连接信息
            service_info = self.plex_service
            plex_instance = service_info.instance
            
            # 获取服务器地址和token
            plex_url = getattr(plex_instance, '_host', None) or getattr(plex_instance, 'host', None)
            plex_token = getattr(plex_instance, '_token', None) or getattr(plex_instance, 'token', None)
            
            if not plex_url or not plex_token:
                logger.error("无法获取Plex服务器连接信息")
                return None
                
            # 确保URL格式正确
            if not plex_url.startswith('http'):
                plex_url = f"http://{plex_url}"
                
            return {
                'url': plex_url.rstrip('/'),
                'token': plex_token
            }
        except Exception as e:
            logger.error(f"获取Plex连接信息失败: {e}")
            return None

    def _get_library_info(self, plex_url: str, plex_token: str) -> Dict[str, Dict[str, Any]]:
        """
        获取Plex库信息，返回库名称到库信息的映射
        """
        try:
            response = requests.get(
                f"{plex_url}/library/sections",
                params={'X-Plex-Token': plex_token},
                timeout=10
            )
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            directories = root.findall('Directory')
            
            library_info = {}
            for directory in directories:
                lib_name = directory.get('title')
                lib_id = directory.get('key')
                lib_type = directory.get('type')  # movie, show等
                
                # 获取库的路径
                locations = [location.get('path') for location in directory.findall('Location')]
                
                library_info[lib_name] = {
                    'id': lib_id,
                    'type': lib_type,
                    'locations': locations
                }
            
            logger.info(f"获取到{len(library_info)}个Plex库")
            return library_info
        except Exception as e:
            logger.error(f"获取Plex库信息失败: {e}")
            return {}

    def _find_matching_library(self, target_path: str, library_info: Dict[str, Dict[str, Any]]) -> Optional[Tuple[str, str]]:
        """
        根据目标路径找到匹配的库
        返回: (库名称, 库ID) 或 None
        """
        target_path = target_path.replace('\\', '/')
        
        best_match = None
        max_match_length = 0
        
        for lib_name, lib_data in library_info.items():
            for location in lib_data['locations']:
                location = location.replace('\\', '/')
                if target_path.startswith(location):
                    if len(location) > max_match_length:
                        max_match_length = len(location)
                        best_match = (lib_name, lib_data['id'])
        
        if best_match:
            logger.info(f"路径 {target_path} 匹配到库: {best_match[0]} (ID: {best_match[1]})")
        else:
            logger.warning(f"未找到匹配路径 {target_path} 的库")
            
        return best_match

    def _refresh_plex_path_http(self, target_path: str) -> bool:
        """
        使用HTTP API刷新指定路径
        """
        connection_info = self._get_plex_connection_info()
        if not connection_info:
            return False
        
        plex_url = connection_info['url']
        plex_token = connection_info['token']
        
        # 获取库信息
        library_info = self._get_library_info(plex_url, plex_token)
        if not library_info:
            return False
        
        # 找到匹配的库
        library_match = self._find_matching_library(target_path, library_info)
        if not library_match:
            return False
        
        lib_name, lib_id = library_match
        
        try:
            # 构造刷新URL
            refresh_url = f"{plex_url}/library/sections/{lib_id}/refresh"
            params = {
                'path': target_path,
                'X-Plex-Token': plex_token
            }
            
            logger.info(f"开始刷新Plex库 '{lib_name}' 路径: {target_path}")
            
            response = requests.get(refresh_url, params=params, timeout=30)
            response.raise_for_status()
            
            if response.status_code == 200:
                logger.info(f"成功触发Plex刷新 - 库: {lib_name}, 路径: {target_path}")
                return True
            else:
                logger.error(f"Plex刷新响应异常 - 状态码: {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP请求失败: {e}")
            return False
        except Exception as e:
            logger.error(f"Plex HTTP刷新失败: {e}")
            return False

    def _get_parent_path(self, file_path: str) -> str:
        """
        获取文件的父目录路径，用于剧集合并
        对于电视剧，通常是剧集所在的季度目录或剧集目录
        """
        path = Path(file_path)
        parent = path.parent
        
        # 如果是视频文件，返回其父目录
        # 如果是目录，返回其自身
        if path.is_file() or any(path.name.endswith(ext) for ext in ['.mkv', '.mp4', '.avi', '.ts', '.m2ts']):
            return str(parent)
        else:
            return str(path)

    def _execute_batch_refresh(self, parent_path: str):
        """
        执行批量刷新
        """
        with self._timer_lock:
            if parent_path not in self._pending_refreshes:
                return
            
            paths_to_refresh = self._pending_refreshes[parent_path].copy()
            # 清理已处理的数据
            del self._pending_refreshes[parent_path]
            if parent_path in self._refresh_timers:
                del self._refresh_timers[parent_path]
        
        if not paths_to_refresh:
            return
        
        logger.info(f"开始批量刷新，父路径: {parent_path}, 包含 {len(paths_to_refresh)} 个文件/目录")
        
        # 使用父目录进行刷新，这样可以覆盖所有子文件
        success = self._refresh_plex_path_http(parent_path)
        
        if success:
            logger.info(f"批量刷新成功: {parent_path}")
        else:
            logger.warning(f"批量刷新失败，尝试逐个刷新")
            # 如果批量刷新失败，尝试逐个刷新
            for path in paths_to_refresh:
                try:
                    self._refresh_plex_path_http(path)
                    time.sleep(1)  # 避免请求过于频繁
                except Exception as e:
                    logger.error(f"单独刷新失败 {path}: {e}")

    def _schedule_batch_refresh(self, plex_path: str):
        """
        安排批量刷新
        """
        parent_path = self._get_parent_path(plex_path)
        
        with self._timer_lock:
            # 添加到待刷新列表
            self._pending_refreshes[parent_path].add(plex_path)
            
            # 如果已经有定时器在运行，取消它
            if parent_path in self._refresh_timers:
                self._refresh_timers[parent_path].cancel()
            
            # 确保延迟时间是数字类型
            delay_seconds = float(self._batch_delay)
            
            # 创建新的定时器
            timer = threading.Timer(
                delay_seconds,
                self._execute_batch_refresh,
                args=[parent_path]
            )
            self._refresh_timers[parent_path] = timer
            timer.start()
            
            logger.info(f"已安排批量刷新: {parent_path} ({len(self._pending_refreshes[parent_path])} 个文件), "
                       f"将在 {delay_seconds} 秒后执行")

    @eventmanager.register(EventType.TransferComplete)
    def refresh(self, event: Event):
        if not self._enabled:
            return
        event_info: dict = event.event_data
        if not event_info:
            return
        if not self.plex_service:
            return
        transferinfo: TransferInfo = event_info.get("transferinfo")
        if not transferinfo or not transferinfo.target_diritem or not transferinfo.target_diritem.path:
            logger.info("未获取到目标路径，跳过Plex刷新")
            return
        local_path = str(transferinfo.target_diritem.path)
        plex_path = self.map_path(local_path)
        if not plex_path:
            logger.info(f"未能映射Plex路径，跳过: {local_path}")
            return
        
        # 标准化路径格式
        plex_path = plex_path.replace("/", "\\") if "\\" in plex_path else plex_path
        logger.info(f"准备刷新Plex路径: {plex_path}")
        
        # 使用批量刷新机制
        self._schedule_batch_refresh(plex_path)

    def stop_service(self):
        """
        停止服务时清理所有定时器
        """
        with self._timer_lock:
            for timer in self._refresh_timers.values():
                if timer.is_alive():
                    timer.cancel()
            self._refresh_timers.clear()
            self._pending_refreshes.clear()
        logger.info("Plex局部刷新插件已停止，所有定时器已清理")