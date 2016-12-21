# bigdata EECSE 6893 
Basically, the program is a python program. You can download the data from here：https://www.kaggle.com/c/expedia-hotel-recommendations/data. The test data dont contain the hotel_cluster , thus if you want to check the accuracy of the recommendation ,you have to split the train data into two parts. Here, training data is “traindata.csv” ,the test data is “testdata.csv”. When you plan 
to run theprogram you have to put the two data files and the program file in the same directory. After running the program, there will
be a file called  “result.csv” produced. The result file would looks like:

The first number represent the user id and the five numbers behind represent the clusters recommended.
We also write a program to check the accuracy of the result. Similarly, you have to put it in the same 
directory with the data file when you run it. 
