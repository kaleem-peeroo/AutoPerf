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

    def __rich_repr__(self):
        if use_reliable:
            use_reliable_str = "REL"
        else:
            use_reliable_str = "BE"

        if use_multicast:
            use_multicast_str = "MC"
        else:
            use_multicast_str = "UC"

        yield "{}SEC_{}B_{}P_{}S_{}_{}_{}DUR_{}LC".format(
            self.duration_secs,
            self.datalen_bytes,
            self.pub_count,
            self.sub_count,
            use_reliable_str,
            use_multicast_str,
            self.durability,
            self.latency_count
        ) 
