import sys
import logging

def error_message_detail(error:Exception, error_detail:sys) ->str:

    #extract traceback details (exception information)
    _, _, exc_tb =error_detail.exc_info()

    #extract the file name where the exception occurred
    file_name = exc_tb.tb_frame.f_code.co_filename


    #create a formatted string with the error message and the file name
    line_number  = exc_tb.tb_lineno
    error_message = f'{error} in {file_name} at line {line_number}'

    logging.error(error_message)

    return error_message

class MyException(Exception):
    def __init__(self, error_message: str , error_detail: sys):
        self.message = error_message
        super().__init__(self.message)
        self.error_detail =error_message_detail(error_message, error_detail)

    def __str__(self):
        return self.message