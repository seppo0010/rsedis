# Check the basic monitoring and failover capabilities.

source "../tests/includes/init-tests.tcl"

xtest "Create a 5 nodes cluster" {
    create_cluster 5 5
}

xtest "Cluster is up" {
    assert_cluster_state ok
}

xtest "Cluster is writable" {
    cluster_write_test 0
}

xtest "Instance #5 is a slave" {
    assert {[RI 5 role] eq {slave}}
}

xtest "Instance #5 synced with the master" {
    wait_for_condition 1000 50 {
        [RI 5 master_link_status] eq {up}
    } else {
        fail "Instance #5 master link status is not up"
    }
}

set current_epoch [CI 1 cluster_current_epoch]

xtest "Killing one master node" {
    kill_instance redis 0
}

xtest "Wait for failover" {
    wait_for_condition 1000 50 {
        [CI 1 cluster_current_epoch] > $current_epoch
    } else {
        fail "No failover detected"
    }
}

xtest "Cluster should eventually be up again" {
    assert_cluster_state ok
}

xtest "Cluster is writable" {
    cluster_write_test 1
}

xtest "Instance #5 is now a master" {
    assert {[RI 5 role] eq {master}}
}

xtest "Restarting the previously killed master node" {
    restart_instance redis 0
}

xtest "Instance #0 gets converted into a slave" {
    wait_for_condition 1000 50 {
        [RI 0 role] eq {slave}
    } else {
        fail "Old master was not converted into slave"
    }
}
