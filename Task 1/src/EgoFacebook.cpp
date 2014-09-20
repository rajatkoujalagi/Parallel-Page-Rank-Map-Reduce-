#include<climits>
#include<fstream>
#include<cmath>
#include<algorithm>
#include<iostream>
#include <stdio.h>
#include<omp.h>
#define d 0.85
#define gamma 0.0000001

using namespace std;
void printArray(int a[]){
	int l=sizeof(a)/sizeof(*a);
    for(int i=0;i<l;i++)
        cout<<a[i]<<" ";
}

void fileReader(){
	FILE *fp;
	char ch;
	int k,v;
	int ctr=0;
	fp = fopen("inp.in","r");
	while(!feof(fp)){
		fscanf(fp,"%d %d",&k,&v);
		//printf("%d %d\n",k,v);
		ctr++;
	
	}
	
	fclose(fp);
	fp = fopen("inp.in","r");
	int key[ctr],value[ctr];
	int i=0;
	while(!feof(fp)){
		fscanf(fp,"%d %d",(key+i),(value+i));
		//printf("%d %d\n",k,v);
		i++;
	
	}
	fclose(fp);
	i=0;
	while(i<ctr){
		printf("%d %d\n",key[i],value[i]);
		i++;
	}
}

int main() {
    ifstream input("facebook_combined.txt");  //reading the input file
    ofstream out("Output_task1.txt");			
    int a, b, i, j;
    int ctr =0;
    int tid, nthreads; 	//thread id and number of threads
    int max = INT_MIN;
    int nValue = 0;
    int chunk = 500;
    double var = 0.0;
    bool flag = false;
    while (input >> a >> b) {   //while we reach the end of file
        max = a > max ? a : max;
        max = b > max ? b : max;
    }
    //printf("%d", max);
    input.clear();
    input.seekg(0, input.beg);  
    max++;
    const int size = max;
    double **adjMatr;  //adjacency matrix
    adjMatr = new double*[size];
    for(i=0;i<size;i++)
    {
    	adjMatr[i] = new double[size];
    }
    double oldPageRank[size],newPageRank[size];
    int count[size];
	//initialize the matrix to 0	
    for (i = 0; i < size; i++) {
        for (j = 0; j < size; j++) {
            adjMatr[i][j] = 0;
        }
        oldPageRank[i] = 1.0;
        newPageRank[i] = 0.0;
        count[i] = 0;
    }
    //reading input file and setting the adjacency matrix
    while (input >> a >> b) {
        adjMatr[a][b] = 1;
        adjMatr[b][a] = 1;
        nValue++;
    }
    nValue *= 2;
    input.close();
    
    //Normalize the matrix
    for(j=0;j<size;j++)
    {
    	for(i=0;i<size;i++)
    	{
    		count[j] = adjMatr[i][j]==1 ? count[j]+1:count[j];
    	}
    }
    
    for(j=0;j<size;j++)
    {
    	for(i=0;i<size;i++)
    	{
    		if(count[j]!=0)
    			adjMatr[i][j] = adjMatr[i][j]/count[j];
    	}
    }
    #pragma omp parallel shared(max,adjMatr,nthreads,chunk,oldPageRank,newPageRank,ctr) private(tid,i,j)
    {
        tid = omp_get_thread_num();
        if (tid == 0) {
            nthreads = omp_get_num_threads();
        }
				#pragma omp for schedule (static,chunk)	
        	for (j = 0; j < size; j++) {
            	oldPageRank[j] = 1.0/(max);
        	}
		
        while (ctr!=89) {
            ctr++;
        		var = 0.0;
        		#pragma omp for schedule (static,chunk)
            for (i = 0; i < size; i++) {
            		newPageRank[i] = 0;
                for (j = 0; j < size; j++) {
                    newPageRank[i] = newPageRank[i] + (adjMatr[i][j] * oldPageRank[j]);
                }
                newPageRank[i] = ((1-d)/(max))+ (d*newPageRank[i]);
            }
           	#pragma omp for schedule (static,chunk)
            for (i = 0; i < max; i++) {
                var += pow(newPageRank[i] - oldPageRank[i], 2);
            }
            #pragma omp for schedule (static,chunk)
            for(i=0;i<max;i++){
            	oldPageRank[i] = newPageRank[i];
            }
            flag = var > (max) * pow(gamma, 2) ? false : true;
            //printf("%d\n",ctr);
        }
     }
    
    
    int min = count[0];
    int minj = 0;
	//sort
    for (i = 0; i < size; i++) {
        out << i << "\t" << newPageRank[i]<<endl;
        min = count[i]<min ? count[i]:min;
        minj = count[i]==min ? i:minj;
    }
    //cout<<endl<<ctr;
	//cout<<"Done";
    return 0;
}
