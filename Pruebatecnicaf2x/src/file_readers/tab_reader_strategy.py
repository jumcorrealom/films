from abc import ABC, abstractmethod

class TabReaderStrategy(ABC):
    
    @abstractmethod
    def read_tabs(self, file_path: str, spark):
        pass
