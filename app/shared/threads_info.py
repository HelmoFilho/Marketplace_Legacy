#=== Importações de módulos externos ===#
import threading

local = threading.local()

class ThreadDictGlobal:

    def save_info_thread(self, **kwargs):

        if hasattr(local, 'value'):
            local.value.update(kwargs)

        else:
            local.value = kwargs


    def remove_info_thread(self, *args):

        if hasattr(local, 'value'):

            for arg in args:

                if arg and isinstance(arg, str):

                    local.value.pop(arg, None)

    
    def get_info_thread(self):

        if hasattr(local, 'value'):
            return local.value
        
        else:
            return {}


    def get(self, arg):

        if hasattr(local, 'value'):

            return local.value.get(f"{arg}", None)
        
        else:
            return {}

    
    def get_mult(self, *args):

        if hasattr(local, 'value'):

            if args:
                return {value: local.value.get(f"{value}", None) for value in args}

            return local.value
        
        else:
            return {}
        
    
    def clean_info_thread(self):

        if hasattr(local, 'value'):
            delattr(local, 'value')