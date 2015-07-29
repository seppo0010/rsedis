start_server {tags {"auth"}} {
    xtest {AUTH fails if there is no password configured server side} {
        catch {r auth foo} err
        set _ $err
    } {ERR*no password*}
}

start_server {tags {"auth"} overrides {requirepass foobar}} {
    xtest {AUTH fails when a wrong password is given} {
        catch {r auth wrong!} err
        set _ $err
    } {ERR*invalid password}

    xtest {Arbitrary command gives an error when AUTH is required} {
        catch {r set foo bar} err
        set _ $err
    } {NOAUTH*}

    xtest {AUTH succeeds when the right password is given} {
        r auth foobar
    } {OK}

    xtest {Once AUTH succeeded we can actually send commands to the server} {
        r set foo 100
        r incr foo
    } {101}
}
