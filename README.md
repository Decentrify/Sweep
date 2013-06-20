
id2210 Decentralized Search Project, vt 2013
===
There are 4 main modules, cyclon, tman, search and election, as well as a common module.
You can run experiments on the search module.

Build instructions:
===
Install git, maven and a java SDK.

cd id2210-vt13
mvn clean install

The search module specifies a kompics experiment in the main experiment.
You can execute the main for the search program by using the assembly plugin on the
search project to generate a jar file containing all code:

cd search
mvn assembly:assembly
java -jar target/id2210-search-1.0-SNAPSHOT-jar-with-dependencies.jar

For further details and possible testing scenarios please refer to the documentation in the class search.main.Main.

The REST API for the search module contains 2 operations:

1. Search for an entry
* http://127.0.1.1:9999/node_id/search-KEYWORDS

2. Add an entry
* http://127.0.1.1:9999/node_id/add-KEYWORD_STRING-MAGNET_LINK

Note that the IP address in the URL might be slightly different - it might be localhost or 127.0.0.1 depending on your OS.
Check in the first lines printed out when running the program.
