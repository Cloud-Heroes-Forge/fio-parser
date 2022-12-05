# from os import popen
import subprocess


class FioRunner:
    @staticmethod
    def run_fio(params: list) -> object:
        param_list = ['fio', '--output-format=json']
        for param in params:
            param_list.append(param[0])
            if param[1]:
                param_list.append(param[1])

        # os_stream = popen('fio {}'.format(param_string))
        fio_process = subprocess.Popen(['fio'] + param_list,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       universal_newlines=True)
        fio_stdout = fio_process.stdout
        fio_stderr = fio_process.stderr

        return fio_stdout
