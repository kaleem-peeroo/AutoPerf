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

    def get_expected_file_count(self):
        return self.sub_count + 1

    def generate_scripts(self):
        if self.pub_count == 0:
            raise ValueError(f"Pub count is 0 for qos: {self.qos_name}")

        if self.sub_count == 0:
            raise ValueError(f"Sub count is 0 for qos: {self.qos_name}")

        data_len_str = f"-dataLen {self.datalen_bytes}"
        durability_str = f"-durability {self.durability}"
        latency_count_str = f"-latencyCount {self.latency_count}"
        exec_time_str = f"-executionTime {self.duration_secs}"

        mc_str = None
        if self.use_multicast:
            mc_str = "-multicast "

        rel_str = None
        if not self.use_reliable:
            rel_str = "-bestEffort "

        script_base = data_len_str + " "

        if rel_str:
            script_base = script_base + rel_str

        if mc_str:
            script_base = script_base + mc_str

        script_base = script_base + durability_str

        script_bases = []

        if self.pub_count == 1:
            pub_script = f"{script_base} -pub -outputFile pub_0.csv"
            pub_script = pub_script + f" -numSubscribers {self.sub_count}"
            pub_script = pub_script + f" {exec_time_str}"
            pub_script = pub_script + f" {latency_count_str}"
            pub_script = pub_script + " -batchSize 0"

            script_bases.append(pub_script)
        else:
            for i in range(self.pub_count):
                # Define the output file for the first publisher.
                if i == 0:
                    pub_script = f"{script_base} -pub"
                    pub_script = pub_script + f" -pidMultiPubTest {i}"
                    pub_script = pub_script + f" -outputFile pub_{i}.csv"
                    pub_script = pub_script + f" -numSubscribers {self.sub_count}"
                    pub_script = pub_script + f" {exec_time_str}"
                    pub_script = pub_script + f" {latency_count_str}"
                    pub_script = pub_script + " -batchSize 0"

                    script_bases.append(pub_script)
                else:
                    pub_script = f"{script_base} -pub"
                    pub_script = pub_script + f" -pidMultiPubTest {i}"
                    pub_script = pub_script + f" -numSubscribers {self.sub_count}"
                    pub_script = pub_script + f" {exec_time_str}"
                    pub_script = pub_script + f" {latency_count_str}"
                    pub_script = pub_script + " -batchSize 0"

                    script_bases.append(pub_script)
        
        if self.sub_count == 1:
            sub_script = f"{script_base} -sub -outputFile sub_0.csv"
            sub_script = sub_script + f" -numPublishers {self.pub_count}"

            script_bases.append(sub_script)
        else:
            for i in range(self.sub_count):
                sub_script = f"{script_base} -sub"
                sub_script = sub_script + f" -sidMultiSubTest {i}"
                sub_script = sub_script + f" -outputFile sub_{i}.csv"
                sub_script = sub_script + f" -numPublishers {self.pub_count}"

                script_bases.append(sub_script)

        scripts = []
        for script_base in script_bases:
            script = f"{script_base} -transport UDPv4"
            scripts.append(script)

        return scripts
