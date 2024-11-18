def get_qos_from_testname(test_name):
    from src.experiments import QoS

    duration_secs = 0
    datalen_bytes = 0
    pub_count = 0
    sub_count = 0
    use_reliable = False
    use_multicast = False
    durability = 0
    latency_count = 0

    if test_name == "":
        raise ValueError("Test name must not be empty")

    if not isinstance(test_name, str):
        raise ValueError(f"Test name must be a string: {test_name}")

    test_name_parts = test_name.split("_")
    if len(test_name_parts) != 8:
        raise ValueError("{} must have 8 parts but has {}".format(
            test_name, len(test_name_parts)
        ))

    for part in test_name_parts:
        if part == "":
            raise ValueError("Test name part must not be empty")

        if part.endswith("SEC"):
            duration_secs = int(part[:-3])

        elif part.endswith("B"):
            datalen_bytes = int(part[:-1])

        elif part.endswith("LC"):
            latency_count = int(part[:-2])

        elif part.endswith("DUR"):
            durability = int(part[:-3])

        elif (part == "UC") or (part == "MC"):

            if part == "UC":
                use_multicast = False
            else:
                use_multicast = True

        elif (part == "REL") or (part == "BE"):

            if part == "REL":
                use_reliable = True
            else:
                use_reliable = False

        elif part.endswith("P"):
            pub_count = int(part[:-1])

        elif part.endswith("S"):
            sub_count = int(part[:-1])

        else:
            raise ValueError(f"Unknown test name part: {part}")

    qos = QoS(
        duration_secs,
        datalen_bytes,
        pub_count,
        sub_count,
        use_reliable,
        use_multicast,
        durability,
        latency_count
    )

    return qos
