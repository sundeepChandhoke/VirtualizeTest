The goal of this prototype is to create a proof-of-concept to demonstrate the idea behind Virtualization of Test.  
The idea here is to apply datacenter technologies to disaggregate the test software components: 
1. Sequencing Engine and the Code Modules that interact with the Device Under Test (DUT), so that the Sequencing Engine can execute on a different compute node from the Code Modules.
2. Additionally, the code modules themselves are disaggregated into Instrument Control IP and Measurement Compute IP.  
3. Instrument Control IP executes on the compute node that hosts the instrumentation connected to the DUT, while the Measurement Compute IP can be executed in any compute target.  
One of the experiments we want try is to horizontally scale the Measurement Compute IP, by executing it in parallel on different compute nodes.  
