This is an ArgoCD "app of apps" configuration to create a test
environment for CRDB-to-CRDB streaming. This directory will build a
source and destination cluster, initialize them, launch a webhook-based
cdc-sink service, and start a simple workload.