As of this writing, migration #3 is safe to run any time you need 
to regenerate the table column type data/data package cache (i.e. 
if things are deleted, or in response to bugs in this process).
You should run the appropriate crawler first.

Migration #5 should be evergreen for managing removal of site data from S3.
