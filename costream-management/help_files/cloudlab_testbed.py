""" Testbed for collecting training for COSTREAM.
 This script can be uploaded at CloudLab, to create the experimental
 infrastructure that was used for collecting the original training data."""

import geni.portal as portal
import geni.rspec.pg as pg
import geni.rspec.emulab as emulab


LAN_BW_IN_GBITS = 10 # We have to specify a bandwidth which is 10Gbit/s for m400 instances
NODE_TYPE = 'm400'

pc = portal.Context()
request = pc.makeRequestRSpec()

# Variable number of nodes at two sites.
pc.defineParameter("clusters", "Number of clusters", portal.ParameterType.INTEGER, 1)
pc.defineParameter("nodes", "Number of nodes per cluster", portal.ParameterType.INTEGER, 6)
params = pc.bindParameters()

# Check parameter validity.
if params.nodes < 2:
    pc.reportError(portal.ParameterError("You must choose at least 2 nodes"))
if params.clusters < 1:
    pc.reportError(portal.ParameterError("You must choose at least one cluster"))

for cluster_index in range(params.clusters):
    ifaces = []
    for node_index in range(params.nodes):
        node = request.RawPC("node" + str(cluster_index) + str(node_index))
        node.disk_image = 'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU20-64-STD'
        node.hardware_type = NODE_TYPE
        node.Site("cluster"+str(cluster_index))
        iface = node.addInterface("eth1")
        iface.addAddress(pg.IPv4Address("192.168." + str(cluster_index + 1) + "." + str(node_index + 1), "255.255.255.0"))
        ifaces.append(iface)

    # Now add the link to the rspec.
    lan = request.LAN("lan"+str(cluster_index))

    # Must provide a bandwidth. BW is in Kbps: m400 instances have 10Gbit/s
    lan.bandwidth = LAN_BW_IN_GBITS * 1000 * 1000

    # Add interfaces to lan
    for iface in ifaces:
        lan.addInterface(iface)

# Print the generated rspec
pc.printRequestRSpec(request)