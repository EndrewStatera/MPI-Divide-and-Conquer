all: sort_parallel

sort_parallel: parallel.c
	mpicc -o sort_parallel parallel.c

clean:
	rm -f sort_parallel

.PHONY: all clean