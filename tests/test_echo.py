import concurrent.futures as cf

_EXPECTED_BYTES = 10589  # arbitrary


def run_tx() -> int:
    return _EXPECTED_BYTES


def run_rx() -> int:
    return _EXPECTED_BYTES


def test_echo() -> None:
    with cf.ProcessPoolExecutor(max_workers=2) as pool:
        tx_future = pool.submit(run_tx)
        rx_future = pool.submit(run_rx)

        for f in cf.as_completed((tx_future, rx_future)):
            assert f.result() == _EXPECTED_BYTES
