from ...refactored.abstract.ExtractorBase import Base
from ...refactored.executable import Executable


class NewMinimizer(Base):

    def __init__(self, connectionHelper, core_relations,name):
        Base.__init__(self, connectionHelper, name)
        self.core_relations = core_relations
        self.app = Executable(connectionHelper)
        
       

