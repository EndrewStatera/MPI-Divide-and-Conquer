#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

int sort_parallel(int VETOR_SIZE, int delta)
{
MPI_Init();
my_rank = MPI_Comm_rank();  // pega o numero do processo atual (rank)

// recebo vetor

if ( my_rank != 0 )
   {
   MPI_Recv ( vetor, pai);                       // não sou a raiz, tenho pai
   MPI_Get_count(&Status, MPI_INT, &tam_vetor);  // descubro tamanho da mensagem recebida
   }
else
   {
   tam_vetor = VETOR_SIZE;               // defino tamanho inicial do vetor
   Inicializa ( vetor, tam_vetor );      // sou a raiz e portanto gero o vetor - ordem reversa
   }

// dividir ou conquistar?

if ( tam_vetor <= delta )
   BubbleSort (vetor);  // conquisto
else
       {
    // dividir
    // quebrar em duas partes e mandar para os filhos

    MPI_Send ( &vetor[0], filho esquerda, tam_vetor/2 );  // mando metade inicial do vetor
    MPI_Send ( &vetor[tam_vetor/2], filho direita , tam_vetor/2 );  // mando metade final

    // receber dos filhos

    MPI_Recv ( &vetor[0], filho esquerda);            
    MPI_Recv ( &vetor[tam_vetor/2], filho direita);   

    // intercalo vetor inteiro
 
    Intercala ( vetor );
    }

// mando para o pai

if ( my_rank !=0 )
   MPI_Send ( vetor, pai, tam_vetor );  // tenho pai, retorno vetor ordenado pra ele
else
   Mostra ( vetor );                    // sou o raiz, mostro vetor

MPI_Finalize();

 
}

void BubbleSort(int *vetor, int tam_vetor) {
    for (int i = 0; i < tam_vetor - 1; i++) {
        for (int j = 0; j < tam_vetor - i - 1; j++) {
            if (vetor[j] > vetor[j + 1]) {
                // Swap vetor[j] and vetor[j + 1]
                int temp = vetor[j];
                vetor[j] = vetor[j + 1];
                vetor[j + 1] = temp;
            }
        }
    }
}


 

 
