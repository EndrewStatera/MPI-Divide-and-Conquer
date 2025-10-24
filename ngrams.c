#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

// Estrutura para armazenar nossa lista dinâmica de strings (tokens ou n-gramas)
typedef struct {
    char **items;
    int size;
    int capacity;
} StringList;

// --- Funções Auxiliares para StringList ---

// Inicializa a lista
void initList(StringList *list, int capacity) {
    list->items = (char **)malloc(capacity * sizeof(char *));
    if (list->items == NULL) {
        fprintf(stderr, "Falha ao alocar memória\n");
        exit(1);
    }
    list->size = 0;
    list->capacity = capacity;
}

// Adiciona um item à lista, redimensionando se necessário
void addToList(StringList *list, const char *item) {
    if (list->size == list->capacity) {
        list->capacity *= 2;
        list->items = (char **)realloc(list->items, list->capacity * sizeof(char *));
        if (list->items == NULL) {
            fprintf(stderr, "Falha ao realocar memória\n");
            exit(1);
        }
    }
    list->items[list->size] = strdup(item); // strdup aloca e copia a string
    if (list->items[list->size] == NULL) {
        fprintf(stderr, "Falha no strdup\n");
        exit(1);
    }
    list->size++;
}

// Libera toda a memória alocada para a lista
void freeList(StringList *list) {
    for (int i = 0; i < list->size; i++) {
        free(list->items[i]); // Libera cada string
    }
    free(list->items); // Libera o vetor de ponteiros
    list->items = NULL;
    list->size = 0;
    list->capacity = 0;
}

// Função de comparação para o qsort
int compareStrings(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

// --- Funções Principais do Algoritmo ---

/**
 * 1. Tokeniza e Normaliza o texto.
 * Quebra o texto em palavras, converte para minúsculo e remove pontuação.
 */
void tokenize(const char *text, StringList *tokens) {
    char *text_copy = strdup(text); // Copia para podermos modificar
    char *buffer = text_copy;
    char *token;

    while ((token = strsep(&buffer, " \n\t\r")) != NULL) {
        if (strlen(token) == 0) continue;

        // Normalização: minúsculo e remoção de pontuação
        char *cleaned_token = (char *)malloc(strlen(token) + 1);
        int j = 0;
        for (int i = 0; token[i]; i++) {
            if (isalpha(token[i])) { // Mantém apenas letras
                cleaned_token[j++] = tolower(token[i]);
            }
        }
        cleaned_token[j] = '\0';

        if (strlen(cleaned_token) > 0) {
            addToList(tokens, cleaned_token);
        }
        free(cleaned_token);
    }
    free(text_copy);
}

/**
 * 2. Gera todos os N-gramas a partir da lista de tokens.
 */
void generateNgrams(StringList *tokens, int N, StringList *ngrams) {
    char ngram_buffer[1024]; // Buffer temporário para construir o N-grama

    for (int i = 0; i <= tokens->size - N; i++) {
        ngram_buffer[0] = '\0'; // Limpa o buffer

        // Concatena N tokens
        for (int j = 0; j < N; j++) {
            strcat(ngram_buffer, tokens->items[i + j]);
            if (j < N - 1) {
                strcat(ngram_buffer, " "); // Adiciona espaço entre as palavras
            }
        }
        addToList(ngrams, ngram_buffer);
    }
}

/**
 * 4. Conta, Filtra e Imprime os N-gramas que atingem o limiar.
 * Esta função assume que a lista 'ngrams' já está ordenada.
 */
void countAndFilter(StringList *sorted_ngrams, int min_threshold) {
    if (sorted_ngrams->size == 0) return;

    int current_count = 1;
    char *current_ngram = sorted_ngrams->items[0];
    int total_ngrams = sorted_ngrams->size;

    printf("--- N-gramas Significativos (Limiar: %d) ---\n", min_threshold);

    for (int i = 1; i < sorted_ngrams->size; i++) {
        if (strcmp(current_ngram, sorted_ngrams->items[i]) == 0) {
            // N-grama é o mesmo, incrementa a contagem
            current_count++;
        } else {
            // N-grama mudou, verifica o anterior
            if (current_count >= min_threshold) {
                double relative_freq = (double)current_count / total_ngrams;
                printf("'%s' \t (Contagem: %d, Frequência: %.4f%%)\n",
                       current_ngram, current_count, relative_freq * 100.0);
            }
            // Reseta para o novo N-grama
            current_ngram = sorted_ngrams->items[i];
            current_count = 1;
        }
    }

    // Verifica o último N-grama do loop
    if (current_count >= min_threshold) {
        double relative_freq = (double)current_count / total_ngrams;
        printf("'%s' \t (Contagem: %d, Frequência: %.4f%%)\n",
               current_ngram, current_count, relative_freq * 100.0);
    }
}

// --- Função Principal ---

int main() {
    // Parâmetros do usuário
    int N = 1; // Queremos Bigramas (N=2)
    int MIN_THRESHOLD = 2; // Mostrar N-gramas que aparecem 3 ou mais vezes

    // Texto de exemplo
    const char *text = 
        "O cachorro correu para o parque. O cachorro feliz brincou no parque. "
        "O gato dormiu no tapete. O cachorro correu de novo, pois o cachorro "
        "é um cachorro muito feliz.";

    StringList tokens;
    StringList ngrams;

    initList(&tokens, 10);
    initList(&ngrams, 10);

    // Passo 1: Tokenizar
    tokenize(text, &tokens);

    /*
    // Descomente para ver os tokens
    printf("--- Tokens ---\n");
    for(int i=0; i<tokens.size; i++) {
        printf("%d: %s\n", i, tokens.items[i]);
    }
    */

    // Passo 2: Gerar N-gramas
    generateNgrams(&tokens, N, &ngrams);

    // Passo 3: Ordenar
    qsort(ngrams.items, ngrams.size, sizeof(char *), compareStrings);

    /*
    // Descomente para ver todos os N-gramas ordenados
    printf("\n--- N-gramas Ordenados ---\n");
    for(int i=0; i<ngrams.size; i++) {
        printf("%d: %s\n", i, ngrams.items[i]);
    }
    */

    // Passo 4: Contar e Filtrar
    countAndFilter(&ngrams, MIN_THRESHOLD);

    // Libera toda a memória
    freeList(&tokens);
    freeList(&ngrams);

    return 0;
}