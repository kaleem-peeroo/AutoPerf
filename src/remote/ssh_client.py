class SSHClient:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def connect(self):
        print(f"Connecting to {self.host} on port {self.port} as {self.user}...")
        # connect to the server

    def execute(self, command):
        print(f"Executing command: {command}")
        # execute the command

    def close(self):
        print(f"Closing connection to {self.host}...")
        # close the connection
