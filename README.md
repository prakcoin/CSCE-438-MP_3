PLEASE NOTE: PLEASE COMPILE ALL .PROTO FILES FIRST AND THEN COMPILE EVERYTHING. SOMETIMES CODE WON'T COMPILE IF COMPILED OUT OF ORDER. APOLOGIES

Compile the code using the provided makefile:

    make

To clear the directory (and remove .txt files):
   
    make clean

To run the coordinator on port 8000:

    ./coordinator -p 8000

To run the master server ID#1 on port 3000 connecting to coordinator 127.0.0.1:8000:

    ./tsd -c 127.0.0.1 -o 8000 -p 3000 -i 1 -t master

To run the client ID#1 connecting to coordinator 127.0.0.1:8000 :

    ./tsc -c 127.0.0.1 -p 8000 -i 1
    
To run the Synchronizer ID#1 connecting to coordinator 127.0.0.1:8000 with port 3000:

    ./synchronizer -c 127.0.0.1 -p 8000 -p 3000 -i 1

