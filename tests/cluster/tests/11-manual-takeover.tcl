# Manual takeover test

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

xtest "Killing majority of master nodes" {
    kill_instance redis 0
    kill_instance redis 1
    kill_instance redis 2
}

xtest "Cluster should eventually be down" {
    assert_cluster_state fail
}

xtest "Use takeover to bring slaves back" {
    R 5 cluster failover takeover
    R 6 cluster failover takeover
    R 7 cluster failover takeover
}

xtest "Cluster should eventually be up again" {
    assert_cluster_state ok
}

xtest "Cluster is writable" {
    cluster_write_test 4
}

xtest "Instance #5, #6, #7 are now masters" {
    assert {[RI 5 role] eq {master}}
    assert {[RI 6 role] eq {master}}
    assert {[RI 7 role] eq {master}}
}

xtest "Restarting the previously killed master nodes" {
    restart_instance redis 0
    restart_instance redis 1
    restart_instance redis 2
}

xtest "Instance #0, #1, #2 gets converted into a slaves" {
    wait_for_condition 1000 50 {
        [RI 0 role] eq {slave} && [RI 1 role] eq {slave} && [RI 2 role] eq {slave}
    } else {
        fail "Old masters not converted into slaves"
    }
}
