class QoS:
    def __init__(
        self,
        duration_secs: int,
        datalen_bytes: int,
        pub_count: int,
        sub_count: int,
        use_reliable: bool,
        use_multicast: bool,
        durability: int,
        latency_count: int
    ):
        self.duration_secs = duration_secs
        self.datalen_bytes = datalen_bytes
        self.pub_count = pub_count
        self.sub_count = sub_count
        self.use_reliable = use_reliable
        self.use_multicast = use_multicast
        self.durability = durability
        self.latency_count = latency_count
        self.qos_name = self.get_qos_name()

    def __rich_repr__(self):
        yield "duration_secs", self.duration_secs
        yield "datalen_bytes", self.datalen_bytes
        yield "pub_count", self.pub_count
        yield "sub_count", self.sub_count
        yield "use_reliable", self.use_reliable
        yield "use_multicast", self.use_multicast
        yield "durability", self.durability
        yield "latency_count", self.latency_count
        yield "qos_name", self.qos_name
        
    def get_qos_name(self):
        if self.use_reliable:
            use_reliable_str = "REL"
        else:
            use_reliable_str = "BE"

        if self.use_multicast:
            use_multicast_str = "MC"
        else:
            use_multicast_str = "UC"

        return "{}SEC_{}B_{}P_{}S_{}_{}_{}DUR_{}LC".format(
            self.duration_secs,
            self.datalen_bytes,
            self.pub_count,
            self.sub_count,
            use_reliable_str,
            use_multicast_str,
            self.durability,
            self.latency_count
        ) 
