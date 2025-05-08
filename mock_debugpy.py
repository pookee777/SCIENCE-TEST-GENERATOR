class MockDebugpy:
    __version__ = 'unknown'
    
    class common:
        @staticmethod
        def json(*args, **kwargs):
            return None
        
        @staticmethod
        def timestamp(*args, **kwargs):
            return 0.0
        
        @staticmethod
        def util(*args, **kwargs):
            return None

debugpy = MockDebugpy()
