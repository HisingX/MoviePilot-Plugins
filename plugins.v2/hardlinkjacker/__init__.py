from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path
import traceback

from app.core.event import eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import ChainEventType, EventType
from app.schemas.event import TransferInterceptEventData
from app.modules.filemanager.storages.local import LocalStorage
from app import schemas


class HardLinkJacker(_PluginBase):
    """
    硬链接劫持器插件 - 用于劫持link方法并使用自定义硬链接逻辑
    """
    
    # 插件名称
    plugin_name = "硬链接劫持器"
    # 插件描述
    plugin_desc = "劫持硬链接操作并使用自定义硬链接逻辑"
    # 插件图标
    plugin_icon = ""
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "HisingX"
    # 作者主页
    author_url = "https://github.com/HisingX"
    # 插件配置项ID前缀
    plugin_config_prefix = "hardlinkjacker_"
    # 加载顺序
    plugin_order = 1
    # 可使用的用户级别
    auth_level = 1

    def __init__(self):
        super().__init__()
        self._enabled = False
        self._intercept_count = 0
        self._original_link_method = None
        self._hijacked = False

    def init_plugin(self, config: Optional[dict] = None):
        """
        生效配置信息
        """
        if config:
            self._enabled = config.get("enabled", False)
        else:
            self._enabled = False
            
        if self._enabled:
            self._hijack_link_method()
            logger.info("【硬链接劫持器】插件已启用，开始劫持硬链接方法")
        else:
            self._restore_link_method()
            logger.info("【硬链接劫持器】插件已禁用")

    def get_state(self) -> bool:
        """
        获取插件运行状态
        """
        return self._enabled
    
    def _hijack_link_method(self):
        """
        劫持LocalStorage的link方法
        """
        if self._hijacked:
            return
            
        try:
            # 保存原始方法
            self._original_link_method = LocalStorage.link
            
            # 获取插件实例
            plugin_instance = self
            
            # 创建替换方法，保持正确的签名
            def custom_link_wrapper(self, fileitem: schemas.FileItem, target_file: Path) -> bool:
                # 这里的self是LocalStorage实例，我们需要调用插件的自定义方法
                return plugin_instance._custom_link_method(fileitem, target_file)
            
            # 替换为自定义方法
            LocalStorage.link = custom_link_wrapper
            self._hijacked = True
            
            logger.info("【硬链接劫持器】成功劫持LocalStorage.link方法")
            
        except Exception as e:
            logger.error(f"【硬链接劫持器】劫持方法失败: {str(e)} - {traceback.format_exc()}")
    
    def _restore_link_method(self):
        """
        恢复原始的link方法
        """
        if not self._hijacked or not self._original_link_method:
            return
            
        try:
            LocalStorage.link = self._original_link_method
            self._hijacked = False
            logger.info("🔄【硬链接劫持器】已恢复原始LocalStorage.link方法")
        except Exception as e:
            logger.error(f"【硬链接劫持器】恢复方法失败: {str(e)}")
    
    def _custom_link_method(self, fileitem: schemas.FileItem, target_file: Path) -> bool:
        """
        自定义硬链接方法，替换原始的SystemUtils.link逻辑
        """
        try:
            self._intercept_count += 1
            
            # 检查文件路径是否为空
            if not fileitem.path:
                logger.error(f"【硬链接劫持器】文件路径为空")
                return False
            
            # 输出劫持成功的日志
            logger.warning(f"【硬链接劫持器】成功劫持硬链接操作! ")
            # 使用自定义硬链接逻辑
            file_path = Path(fileitem.path)
            code, message = self._custom_link_logic(file_path, target_file)
            
            if code != 0:
                logger.error(f"【硬链接劫持器】自定义硬链接失败：{message}")
                return False
            else:
                logger.info(f"【硬链接劫持器】硬链接成功!")
                return True
                
        except Exception as e:
            logger.error(f"【硬链接劫持器】自定义硬链接方法执行出错: {str(e)} - {traceback.format_exc()}")
            return False
    
    @staticmethod
    def _custom_link_logic(src: Path, dest: Path) -> Tuple[int, str]:
        """
        自定义硬链接逻辑（不加后缀，直接检测目标文件是否存在）
        """
        try:
            # logger.info(f"【硬链接劫持器】执行自定义硬链接逻辑: {src} -> {dest}")
            
            # 检查目标路径是否已存在，如果存在则先unlink
            if dest.exists():
                logger.info(f"【硬链接劫持器】目标文件已存在，先删除: {dest}")
                dest.unlink()
                
            # 执行硬链接
            logger.info(f"【硬链接劫持器】创建硬链接: {src} -> {dest}")
            dest.hardlink_to(src)
            
            # logger.info(f"【硬链接劫持器】硬链接创建成功!")
            return 0, ""
            
        except Exception as err:
            error_msg = f"自定义硬链接失败: {str(err)}"
            logger.error(f"【硬链接劫持器】{error_msg}")
            return -1, error_msg

    @eventmanager.register(ChainEventType.TransferIntercept)
    def transfer_intercept(self, event):
        """
        拦截文件整理操作
        """
        if not self._enabled:
            return
            
        event_data: TransferInterceptEventData = event.event_data
        if not event_data:
            return

    def get_api(self) -> List[Dict[str, Any]]:
        """
        获取插件API
        """
        return [
            {
                "path": "/status",
                "endpoint": self.get_status,
                "methods": ["GET"],
                "summary": "获取插件状态",
                "description": "获取硬链接劫持器插件的运行状态"
            }
        ]

    def get_status(self):
        """
        获取插件状态
        """
        return {
            "enabled": self._enabled,
            "intercept_count": self._intercept_count,
            "message": "硬链接劫持器插件运行正常"
        }

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                            'hint': '开启后将监听并记录所有文件操作',
                                            'persistent-hint': True
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
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal'
                                        },
                                        'content': [
                                            {
                                                'component': 'span',
                                                'text': '这是一个硬链接方法劫持器插件，针对Windows通过NFS方式共享给linux磁盘后，原代码中会出现硬链接刮削乱码的问题进行修复。'
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False
        }

    def get_page(self) -> List[dict]:
        """
        拼装插件详情页面
        """
        return [
            {
                'component': 'VRow',
                'content': [
                    {
                        'component': 'VCol',
                        'props': {
                            'cols': 12
                        },
                        'content': [
                            {
                                'component': 'VCard',
                                'props': {
                                    'variant': 'tonal'
                                },
                                'content': [
                                    {
                                        'component': 'VCardTitle',
                                        'props': {
                                            'class': 'text-h6'
                                        },
                                        'text': '硬链接劫持器状态'
                                    },
                                    {
                                        'component': 'VCardText',
                                        'content': [
                                            {
                                                'component': 'VList',
                                                'content': [
                                                    {
                                                        'component': 'VListItem',
                                                        'props': {
                                                            'title': '插件状态',
                                                            'subtitle': '启用' if self._enabled else '禁用'
                                                        }
                                                    },
                                                    {
                                                        'component': 'VListItem',
                                                        'props': {
                                                            'title': '拦截次数',
                                                            'subtitle': str(self._intercept_count)
                                                        }
                                                    }
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        定义远程控制命令
        """
        return [
            {
                "cmd": "/hijacker_status",
                "event": EventType.PluginAction,
                "desc": "查看硬链接劫持器状态",
                "data": {"action": "status"}
            }
        ]

    def stop_service(self):
        """
        退出插件
        """
        if self._hijacked:
            self._restore_link_method()
        logger.info("【硬链接劫持器】插件服务已停止")
