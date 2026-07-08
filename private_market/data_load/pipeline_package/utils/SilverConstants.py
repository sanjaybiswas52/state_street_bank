class SilverConstants:
    """
    Constants for Silver tables.
    """
    REJECTED_AUDIT = "REJECTED_AUDIT"



    @classmethod
    def as_dict(cls):
        return {
            k: v for k, v in cls.__dict__.items() 
            if not k.isupper() and not k.startswith('__') 
        }
    