start_server {} {
    set i [r info]
    regexp {rsedis_version:(.*?)\r\n} $i - version
    regexp {rsedis_git_sha1:(.*?)\r\n} $i - sha1
    puts "Testing Redis version $version ($sha1)"
}
