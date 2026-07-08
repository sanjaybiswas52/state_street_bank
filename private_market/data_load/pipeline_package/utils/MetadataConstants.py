class SilverConstants:
    """
    Constants for Silver tables.
    """
    DIM_ENTITY_MASTER_TBL = "dim_entity_master"



    @classmethod
    def as_dict(cls):
        return {
            k: v for k, v in cls.__dict__.items() 
            if not k.isupper() and not k.startswith('__') 
        }
    