all: parallel

parallel: parallel.cpp
	mpic++ -o parallel parallel.cpp -std=c++11

clean:
	rm -f parallel

.PHONY: all clean