from amqpstorm.management.base import ManagementHandler

HEALTHCHECKS = 'healthchecks/node/'
HEALTHCHECKS_NODE = 'healthchecks/node/%s'


class HealthChecks(ManagementHandler):
    def get(self, node=None):
        """Run basic healthchecks against the current node, or against a given
        node.

            Example response:
                > {"status":"ok"}
                > {"status":"failed","reason":"string"}

        :param node: Node name

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :rtype: dict
        """
        if not node:
            return self.http_client.get(HEALTHCHECKS)
        return self.http_client.get(HEALTHCHECKS_NODE % node)
