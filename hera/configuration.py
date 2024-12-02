class configuration:
    # This class is used to define the configuration 
    # within the parameters space of the experiment

    def __init__(self, version: int, product: int, step: int):
        self.version = version
        self.product = product
        self.step = step

    def __str__(self):
        return f"ve{self.version}-pr{self.product}-st{self.step}"
    
    def __repr__(self):
        return str(self)

    def to_dict(self):
        return {
            "version": self.version,
            "product": self.product,
            "step": self.step
        }
