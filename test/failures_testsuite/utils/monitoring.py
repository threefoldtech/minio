from jumpscale import j

logger = j.logger.get('s3demo')


class Monitoring:

    def __init__(self, parent):
        self._parent = parent

    def start_rtinfo(self, host, port=9930):
        """
        start rtinfo on all the nodes used by the s3 enviroments

        :param host: ip of rtinfod
        :type host: str
        :param port: port of rtinfod, defaults to 9930
        :param port: int, optional
        """
        def do(node):
            if "%s:%s" % (host, port) not in node.client.rtinfo.list():
                logger.info("start rtinfo on node %s", node.name)
                node.client.rtinfo.start(host, port, ['sd'])
        self._parent.execute_all_nodes(do)

    def stop_rtinfo(self, host, port=9930):
        """
        stop rtinfo on all the nodes used by the s3 enviroments

        :param host: ip of rtinfod
        :type host: str
        :param port: port of rtinfod, defaults to 9930
        :param port: int, optional
        """
        def do(node):
            if "%s:%s" % (host, port) in node.client.rtinfo.list():
                logger.info("stop rtinfo on node %s", node.name)
                node.client.rtinfo.stop(host, port)
        self._parent.execute_all_nodes(do)
