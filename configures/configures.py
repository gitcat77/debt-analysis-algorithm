import sys
import configparser
import codecs
import os.path


class Configures(object):
    _instance = None

    def __new__(cls, *args, **kw):
        if not cls._instance:
            cls._instance = configparser.ConfigParser()

            # 配置文件的可能路径
            default_conf_paths = [os.path.dirname(os.path.realpath(sys.executable)) + '/application.conf',
                                  sys.path[0] + '/../application.conf']

            for conf_path in default_conf_paths:
                if os.path.isfile(conf_path):
                    cls._instance.read_file(codecs.open(conf_path, "r", "utf-8-sig"))
                    break
                # else:
                #     print('conf_path:')
                #     print(conf_path)
                #     print('conf_path-0:')
                #     print(sys.path[0])
                #     print('conf_path-1:')
                #     print(sys.path)
                #     print('conf_path-2:')
                #     print(os.path.abspath(__file__))
                #     print('conf_path-3:')
                #     print(os.path.dirname(os.path.abspath(__file__)))
                #     print('new_path-1:')
                #     print(sys.path[0])
                #     print('new_path-2:')
                #     print(sys.argv[0])
                #     print('new_path-3:')
                #     print(os.path.dirname(os.path.realpath(sys.executable)))
                #     print('new_path-4:')
                #     print(os.path.dirname(os.path.realpath(sys.argv[0])))

            if not cls._instance.has_section('system'):
                raise RuntimeError('未找到正确的配置文件：application.conf')
        return cls._instance


# 配置文件对象
configures = Configures()


# 获取日志目录
def configures_sys_log_path():
    sys_log_dir = configures.get('system', 'sys_log')
    if sys_log_dir.startswith('~'):
        sys_log_dir = os.path.expanduser('~') + sys_log_dir[1:]

    log_dir_path = os.path.split(sys_log_dir)[0]
    if not os.path.exists(log_dir_path):
        os.makedirs(log_dir_path)
    return sys_log_dir


def main():
    pass


if __name__ == "__main__":
    main()
