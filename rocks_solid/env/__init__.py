
class BaseChecker(object) :
    def __init__(self, config) :
        self.config = config
    
    def check(self) :
        '''
        Check for environmental anomally

        @rtype int
        @return 0 if normal, other value if error
        '''
        pass

class BaseAction(object) :
    def __init__(self, config) :
        self.config = config

    def act(self, level='') :
        '''
        Initiate action when something goes wrong
        '''
        pass
