# external-sorting

## Implementation of external sorting

The idea behind this project is to allow the sorting of very big text files that might not fit into memory, 
by splitting the file into smaller files, and then sorting and merging those.
It does not try to guesstimate the best use of memory, it just uses a value for the sort block size based on
some local tests.

**Reading and splitting**

Reading from the original file is done sequentially into a list of Strings that then will be passed into a 
background thread to sort them and write the result into a temporary file. The temporary files are gathered into
a list and this list of files is then merge-sorted int a final output file.

**Merging and sorting**

The merge-sort is done by using a PriorityQueue that reads (and sorts) each line from the list of all temporary files.
After each read the file is re-inserted into the PQ until it's empty. Th process continues until all files are read.
To allow this to work, a wrapper around BufferedReader was created to allow a "peek" into the file, when 
adding the file to the PQ. The sorting will be called when the file is added to the PQ, but we don't want to advance 
the file position when doing that, or we'd lose data.

