class HostAddress:

    def __init__(self, host: str, port: int = 3306):
        self.host = host
        self.port = port

    @property
    def primary(self) -> bool:
        return True

    @staticmethod
    def parse(spec: str) -> list:
        if "" == spec:
            return []

        tokens = spec.strip().split(",")

        arr = []
        for i in range(len(tokens)):
            token = tokens[i]
            port_value = 3306
            if token[0] == '[':
                # IPv6 addresses in URLs are enclosed in square brackets
                ind = token.find(']')
                host = token[1:ind]
                if ind != len(token) - 1 and token[ind + 1] == ':':
                    port_value = int(token.substring(ind + 2))
            elif token.find(":") > 0:
                # Parse host:port
                host_port = token.split(":")
                host = host_port[0];
                port_value = int(host_port[1])
            else:
                # Just host name is given
                host = token
            arr.append(HostAddress(host, port_value))
        return arr
