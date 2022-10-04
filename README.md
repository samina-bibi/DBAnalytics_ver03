# DBAnalytics_ver03
Code Runs Tests on Database (DB) Appending speeds for various DB and Binary Blob Sizes. This is done versus the controlled FileOutputStream method. The results show that the control case is much faster and less heavy than the SQLite DB version. The DB version takes 0.5 s on average to load an entire DB, whereas the later takes less than 1 ms. This may be the result of wrongly coding for the FileOutputStream tests.

To RUN: Open project on Eclipse. Run in order InitializeDB > PopulateDB > DataAnlysis > FileStream. Each process will print out a message indicating completion (ex: "Database Initialized", Database Populated").

Caution: Please do not run PopulateDB twice. You must first initialize the database through InitializeDB then repopulate it through PopulatedDB. Not doing so will produce an unclean database which will obscure data analysis.
