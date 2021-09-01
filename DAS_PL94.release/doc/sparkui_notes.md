# Making the SparkUI work

This document describes how to get the SparkUI work with manual port forwarding using Cygwin and SSH running on your local Windows machine. It's a bit awkward, but it should always work.

## Finding the URL from the Master node

When Spark starts up, you'll see some lines that look like this:
```
19/03/19 23:04:24 INFO Utils: Successfully started service 'SparkUI (HTTPS)' on port 4440.
19/03/19 23:04:24 INFO Utils: Successfully started service 'SparkUI' on port 4040.
19/03/19 23:04:24 INFO SparkUI: Bound SparkUI to 10.252.46.163, and started at http://ip-10-252-46-163.ite.ti.census.gov:4040
```
This implies that we can get to the SparkUI with https://10.252.46.163:4440/. However, that's incorrect. We can verify that using the `curl` command to attempt to fetch information from that URL. We need to add the `--insecure` option because we do not want to check TLS certificates. Also, because the name `ip-10-252-46-163.ite.ti.census.gov` may not be set up in the DNS, we'll use the IP address instead:

```
$ curl --insecure https://10.252.46.163:4440/
$ 
```

See?  No output. Now we'll run it with localhost instead:

```
$ curl --insecure https://localhost:4440/
<html>
  <head>
    <title>
      Moved
    </title>
  </head>
  <body>
    <h1>
      Moved
    </h1>
    <div>
      Content has moved 
      <a href="http://ip-10-252-46-163.ite.ti.census.gov:20888/proxy/application_1548266612000_0883/">here</a>
    </div>
  </body>
</html>
$
```

It looks like the Spark UI is only responding to requests on localhost, despite what the log message says. Also, it's telling us to go to a different URL with a different port.

Let's check at the recommended address to see if there is a UI running there. Again we'll use `curl`. This time, though, we'll just look at the first 10 lines of output:

```
$ curl http://ip-10-252-46-163.ite.ti.census.gov:20888/proxy/application_1548266612000_0883 | head -10
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
  0     0    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0<!DOCTYPE html><html>
      <head>
        <meta http-equiv="Content-type" content="text/html; charset=utf-8"/><link rel="stylesheet" href="/proxy/application_1548266612000_0883/static/bootstrap.min.css" type="text/css"/><link rel="stylesheet" href="/proxy/application_1548266612000_0883/static/vis.min.css" type="text/css"/><link rel="stylesheet" href="/proxy/application_1548266612000_0883/static/webui.css" type="text/css"/><link rel="stylesheet" href="/proxy/application_1548266612000_0883/static/timeline-view.css" type="text/css"/><script src="/proxy/application_1548266612000_0883/static/sorttable.js"></script><script src="/proxy/application_1548266612000_0883/static/jquery-1.11.1.min.js"></script><script src="/proxy/application_1548266612000_0883/static/vis.min.js"></script><script src="/proxy/application_1548266612000_0883/static/bootstrap-tooltip.js"></script><script src="/proxy/application_1548266612000_0883/static/initialize-tooltips.js"></script><script src="/proxy/application_1548266612000_0883/static/table.js"></script><script src="/proxy/application_1548266612000_0883/static/additional-metrics.js"></script><script src="/proxy/application_1548266612000_0883/static/timeline-view.js"></script><script src="/proxy/application_1548266612000_0883/static/log-view.js"></script><script src="/proxy/application_1548266612000_0883/static/webui.js"></script><script>setUIRoot('/proxy/application_1548266612000_0883')</script>
        
        
        <title>DAS_RI_TEST - Spark Jobs</title>
      </head>
      <body>
        <div class="navbar navbar-static-top">
          <div class="navbar-inner">
100 26079    0 26079    0     0  26079      0 --:--:-- --:--:-- --:--:--  749k
curl: (23) Failed writing body (2593 != 16384)
teats-ITE-MASTER:hadoop@imrdasem2ea3$ 
```

Okay that totally worked.  Note:

* Good news --- it's http, not https. That will make it easier to proxy.
* So we could just run an SSH proxy on our local machine and proxy through to port 20888. 
* But note that we need to go to the directory `/proxy/application_1548266612000_0883`. It turns out that going to the root web page won't work.

Now we are ready to proxy!

On my Windows client, I fire up Cygwin and type this int he window:

    $ ssh -A hadoop@10.252.46.163 -L20888:ip-10-252-46-163.ite.ti.census.gov:20888

I've already set up ssh public keys so that the `hadoop` user trusts me, but you could have just put in your admin account instead. The `-L` argument tells SSH to listen to port 20888 on your local computer and forward that to the other end of the SSH connection, then to connect to the computer ip-10-252-46-163.ite.ti.census.gov (which works in GovCloud, apparently) and to port 20888 on that machine. Basically, this is the same URL that we tested above with the `curl` command.

To use the Spark UI, I open this url on my Windows host:

    http://localhost:20888/proxy/application_1548266612000_0883

And it works! Take a look:

![Happy Browser](images/sshproxy1.png)


## What's listening
When Spark is running, the following ports are listening on the master node:
```
$ netstat -l | grep LISTEN | grep tcp
tcp        0      0 ip-10-252-46-163.ite.:10020 *:*                         LISTEN      
tcp        0      0 *:54981                     *:*                         LISTEN      
tcp        0      0 ip-10-252-46-163.ite.:50470 *:*                         LISTEN      
tcp        0      0 imrdasem2ea3:smux           *:*                         LISTEN      
tcp        0      0 imrdasem2ea3:43053          *:*                         LISTEN      
tcp        0      0 *:sunrpc                    *:*                         LISTEN      
tcp        0      0 ip-10-252-46-163.ite.:19888 *:*                         LISTEN      
tcp        0      0 *:10033                     *:*                         LISTEN      
tcp        0      0 ip-10-252-4:intu-ec-svcdisc *:*                         LISTEN      
tcp        0      0 *:domain                    *:*                         LISTEN      
tcp        0      0 *:ssh                       *:*                         LISTEN      
tcp        0      0 imrdasem2ea3:ipp            *:*                         LISTEN      
tcp        0      0 imrdasem2ea3:smtp           *:*                         LISTEN      
tcp        0      0 ip-10-252-46-163.ite.t:8030 *:*                         LISTEN      
tcp        0      0 imrdasem2ea3:35103          *:*                         LISTEN      
tcp        0      0 *:18080                     *:*                         LISTEN      
tcp        0      0 ip-10-252-46-163.ite:pro-ed *:*                         LISTEN      
tcp        0      0 ip-10-252-46-163.:mindprint *:*                         LISTEN      
tcp        0      0 *:tnp                       *:*                         LISTEN      
tcp        0      0 *:board-roar                *:*                         LISTEN      
tcp        0      0 imrdasem2ea3:9701           *:*                         LISTEN      
tcp        0      0 *:8998                      *:*                         LISTEN      
tcp        0      0 ip-10-252-46-163.ite.:33479 *:*                         LISTEN      
tcp        0      0 *:yo-main                   *:*                         LISTEN      
tcp        0      0 *:sunrpc                    *:*                         LISTEN      
tcp        0      0 *:18480                     *:*                         LISTEN      
tcp        0      0 *:scotty-ft                 *:*                         LISTEN      
tcp        0      0 ip-10-252-46-163.ite.:46417 *:*                         LISTEN      
tcp        0      0 imrdasem2ea3:sua            *:*                         LISTEN      
tcp        0      0 *:domain                    *:*                         LISTEN      
tcp        0      0 *:57365                     *:*                         LISTEN      
tcp        0      0 *:ssh                       *:*                         LISTEN      
tcp        0      0 localhost6:ipp              *:*                         LISTEN      
tcp        0      0 *:4440                      *:*                         LISTEN      
tcp        0      0 ip-10-252-46-163:radan-http *:*                         LISTEN      
tcp        0      0 ip-10-252-46-163.it:trisoap *:*                         LISTEN      
tcp        0      0 ip-10-252-46-163.ite.:20888 *:*                         LISTEN      
tcp        0      0 ip-10-252-46-16:ca-audit-da *:*                         LISTEN      
tcp        0      0 *:pcsync-https              *:*                         LISTEN      
tcp        0      0 ip-10-252-46-163.ite.t:8188 *:*                         LISTEN      
tcp        0      0 *:pcsync-http               *:*                         LISTEN      
$ 
```

The table below will document what the purpose of some of these ports are:

|Port|Purpose|SSL?|
|----|-------|----|
|8088|Hadoop Applications|NO|
|50070|Hadoop Cluster Overview|NO|
|19888|Hadoop JobHistory|NO|
|18480|Spark history Port|NO|


