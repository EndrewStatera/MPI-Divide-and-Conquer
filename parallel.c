#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

#define DEBUG 1   
#define ARRAY_SIZE 40      // trabalho final com o valores 10.000, 100.000, 1.000.000


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


void Inicializa(int *vetor, int tam_vetor){
    int i;

    for (i=0 ; i<tam_vetor; i++)              /* init array with worst case for sorting */
        vetor[i] = tam_vetor - i;
   

    #ifdef DEBUG
    printf("\nVetor: ");
    for (i=0 ; i<tam_vetor; i++)              /* print unsorted array */
        printf("[%03d] ", vetor[i]);
      printf("\n");
    #endif
}



int *Intercala(int vetor[], int tam)
{
	int *vetor_auxiliar;
	int i1, i2, i_aux;

	vetor_auxiliar = (int *)malloc(sizeof(int) * tam);

	i1 = 0;
	i2 = tam / 2;

	for (i_aux = 0; i_aux < tam; i_aux++) {
		if (((vetor[i1] <= vetor[i2]) && (i1 < (tam / 2)))
		    || (i2 == tam))
			vetor_auxiliar[i_aux] = vetor[i1++];
		else
			vetor_auxiliar[i_aux] = vetor[i2++];
	}

	return vetor_auxiliar;
}

void Mostra(int vetor[]){
   printf("Mostrando vetor:\n");
   for(int i = 0; i < ARRAY_SIZE; i++){
      printf("%d ", vetor[i]);
   }
   printf("\n");
}


int sort_parallel(int delta) {
  MPI_Init(NULL, NULL);

  int my_rank, nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  MPI_Status status;

  int tam_vetor;
  int rank_pai;
  int *vetor; 

  if (my_rank != 0) {
   // não sou a raiz, tenho pai, recebo o tamanho do vetor
    MPI_Recv(&tam_vetor, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status); 
    rank_pai = status.MPI_SOURCE;

    vetor = (int *)malloc(tam_vetor * sizeof(int));
    // agora recebo o vetor em si do pai
    MPI_Recv(vetor, tam_vetor, MPI_INT, rank_pai, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

  } else {
    // sou raiz, tenho que inicializar o vetor
    tam_vetor = ARRAY_SIZE; // defino tamanho inicial do vetor
    vetor = (int *)malloc(tam_vetor * sizeof(int));
    Inicializa(vetor, tam_vetor); // sou a raiz e portanto gero o vetor - ordem reversa
  }

  int filho_esquerda = (2 * my_rank) + 1;
  int filho_direita = (2 * my_rank) + 2;

  // dividir ou conquistar?

  if (tam_vetor <= delta || filho_esquerda >= nprocs)
    BubbleSort(vetor, tam_vetor); // conquisto
  else {
    // quebrar em duas partes e mandar para os filhos
   #ifdef DEBUG
    printf("[Rank %d] Dividindo. Esq: %d | Dir: %d \n", my_rank, filho_esquerda, filho_direita);
   #endif
    // envio primeiro o tamanho do vetor pros filhos
    int tam_vetor_filhos = tam_vetor / 2;
    MPI_Send(&tam_vetor_filhos, 1, MPI_INT, filho_esquerda, 0, MPI_COMM_WORLD);
    MPI_Send(&tam_vetor_filhos, 1, MPI_INT, filho_direita, 0, MPI_COMM_WORLD);
    
    // depois envio o vetor em si
    MPI_Send(vetor, tam_vetor_filhos, MPI_INT, filho_esquerda, 0, MPI_COMM_WORLD); // mando metade inicial do vetor
    MPI_Send(&vetor[tam_vetor_filhos], tam_vetor_filhos, MPI_INT, filho_direita, 0, MPI_COMM_WORLD); // mando metade final

    // receber dos filhos

    MPI_Recv(vetor, tam_vetor_filhos, MPI_INT, filho_esquerda, 0, MPI_COMM_WORLD, &status);
    MPI_Recv(&vetor[tam_vetor_filhos], tam_vetor_filhos, MPI_INT, filho_direita, 0, MPI_COMM_WORLD, &status);

    // intercalo vetor inteiro

    #ifdef DEBUG
    printf("[Rank %d] Intercalando.\n", my_rank);
    #endif

    int *vetor_ordenado = Intercala(vetor, tam_vetor);
    free(vetor);
    vetor = vetor_ordenado;
  }

  // mando para o pai

  if (my_rank != 0)
    MPI_Send(vetor, tam_vetor, MPI_INT, rank_pai, 0, MPI_COMM_WORLD); // tenho pai, retorno vetor ordenado pra ele
  else
    Mostra(vetor); // sou o raiz, mostro vetor

  free(vetor);
  MPI_Finalize();
  return 0;
}

int main(){
   int delta = 5;
   sort_parallel(delta);
}


