#include<stdio.h>
#include<stdlib.h>
#include<mpi.h>
#include <string.h>


int numnodes,myid,mpi_err;

#define mpi_root 0


void init_it(int  *argc, char ***argv);

void init_it(int  *argc, char ***argv) {
    mpi_err = MPI_Init(argc,argv);
    mpi_err = MPI_Comm_size( MPI_COMM_WORLD, &numnodes );
    mpi_err = MPI_Comm_rank(MPI_COMM_WORLD, &myid);
}

int main(int argc, char *argv[])
{

	int *array,*send_ray,*back_ray; int *receive_buffer; int *send_buffer;
	int size,mysize,k,total,gtotal;
	int count=1;
        int *keys; int *keys_source;
	int *values; int *values_source;
	int n,i,j,temp,previous;
        int *fin_keys; int  *fin_values;
        int *global_i;
        char *output_file;	
        int displacements[numnodes]; 
	init_it(&argc,&argv);

if (myid == 0) {
         FILE *f = fopen(argv[1], "rb"); // reading the input file from the console
	fseek(f, 0, SEEK_END);
	long pos = ftell(f);
	fseek(f, 0, SEEK_SET);
	char *line;
	char *bytes = malloc(pos);
	fread(bytes, pos, 1, f);
	fclose(f);

	int i=0;
	int count=0;
        output_file=argv[2];
	for (; bytes[i];  i++ )
		if(bytes[i]=='\n')
 			count++;
        n=count;
        array=(int *)malloc(sizeof(int)*2*count);
	
         j=0;k=count;
	i=0;
	count=0;
	line=strtok(bytes, ",\n");
	while (line)
	{
		array[i++]=atoi(line);

		line=strtok(NULL, ",\n");
                count++;
	}
	free(bytes); 

}
//setting the barrier for wait
MPI_Barrier(MPI_COMM_WORLD);
//broadcasting the message
MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);

receive_buffer=(int*)malloc(sizeof(int)*2*(n/numnodes+n%numnodes));

keys=(int*)malloc(sizeof(int)*(n/numnodes+n%numnodes));
values=(int*)malloc(sizeof(int)*(n/numnodes+n%numnodes));
global_i=(int *)malloc(sizeof(int)*numnodes);
MPI_Barrier(MPI_COMM_WORLD);

//split data to each processor
mpi_err = MPI_Scatter(array,2*n/numnodes,   MPI_INT, receive_buffer,    2*n/numnodes,   MPI_INT,  0, MPI_COMM_WORLD);


j=0;
for(i=0;i<2*n/numnodes;i++)
{
if(i%2==0)
keys[j]=receive_buffer[i];
else
{
values[j]=receive_buffer[i];
j++;
}
}

if(myid==0)
{

for(i=2*n-2*(n%numnodes);i<2*n;i++)
{
if(i%2==0)
keys[j]=array[i];
else
{
values[j]=array[i];
j++;
}

}

}

int len;
if(myid!=0)
len=n/numnodes; 
else
len=(n/numnodes+n%numnodes);

for(i=0;i<len;i++)
for(j=i+1;j<len;j++)
if(keys[j]<keys[i])
{
temp=keys[i];
keys[i]=keys[j];
keys[j]=temp;
temp=values[i];
values[i]=values[j];
values[j]=temp;
}





previous=0;
for(i=1;i<len;i++)
if(keys[previous]==keys[i])
{
values[previous]+=values[i];
}
else
{
values[++previous]=values[i];
keys[previous]=keys[i];
}

send_buffer=(int *)malloc((previous+1)*2*sizeof(int));
i=0;j=0;
for(;j<previous+1;i++)
{
if(i%2==0)
send_buffer[i]=keys[j];
else
{
send_buffer[i]=values[j];
j++;
}

}




MPI_Gather(&previous, 1, MPI_INT, global_i, 1, MPI_INT, 0, MPI_COMM_WORLD);

if(myid == mpi_root){
		displacements[0]=0;
               for(i=0;i<numnodes;i++)
                   global_i[i]=2*(global_i[i]+1);
            		for( i=1;i<numnodes;i++){
			displacements[i]=global_i[i-1]+displacements[i-1];
		}
		size=0;
		for(i=0;i< numnodes;i++)
			size=size+global_i[i];
                
                send_ray=(int*)malloc(size*sizeof(int));
                fin_keys=(int*)malloc(size/2*sizeof(int));
                fin_values=(int*)malloc(size/2*sizeof(int));
	}

MPI_Barrier(MPI_COMM_WORLD);
int count_elem=previous+1;
mpi_err = MPI_Gatherv(send_buffer, 2*count_elem,  MPI_INT, send_ray,global_i,displacements,MPI_INT,  mpi_root, MPI_COMM_WORLD);




if(myid == mpi_root){
            
             j=0;
             
             for(i=0;i<size;i++)
            {
            if(i%2==0)
             fin_keys[j]=send_ray[i];
            else
             fin_values[j++]=send_ray[i];
             }               
                          

               for(i=0;i<size/2;i++)
			for(j=i+1;j<size/2;j++)
				if(fin_keys[j]<fin_keys[i])
				{
					temp=fin_keys[i];
					fin_keys[i]=fin_keys[j];
					fin_keys[j]=temp;
					temp=fin_values[i];
					fin_values[i]=fin_values[j];
					fin_values[j]=temp;
				}

		previous=0;
		for(i=1;i<size/2;i++)
			if(fin_keys[previous]==fin_keys[i])
			{
				fin_values[previous]+=fin_values[i];
			}
			else
			{
				fin_values[++previous]=fin_values[i];
				fin_keys[previous]=fin_keys[i];

			}
              FILE  *f = fopen("output_task2.txt", "w+");   //writing it to the ouput file
              if (f == NULL)
		err(1, "output_task2.txt");
 
              for(i=0;i<previous+1;i++)
		fprintf(f, "%d,%d\n",fin_keys[i],fin_values[i]);
             fclose(f);

		
		}
     
    mpi_err = MPI_Finalize();
}

