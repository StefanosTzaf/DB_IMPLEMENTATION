bplus:
	@echo " Compile bplus_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bp_main.c ./src/record.c ./src/bp_file.c ./src/bp_datanode.c ./src/bp_indexnode.c -lbf -o ./build/bplus_main -O2
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bp_test1.c ./src/record.c ./src/bp_file.c ./src/bp_datanode.c ./src/bp_indexnode.c -lbf -o ./build/bp_test1 -O2
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bp_test2.c ./src/record.c ./src/bp_file.c ./src/bp_datanode.c ./src/bp_indexnode.c -lbf -o ./build/bp_test2 -O2


bp_main:
	@echo " Run bplus_main ...";
	./build/bplus_main

test1:
	@echo " Run test1 ...";
	./build/bp_test1
	
test2:
	@echo " Run test2 ...";
	./build/bp_test2

clean:
	rm -f ./build/bplus_main
	rm -f ./build/bp_test1
	rm -f ./build/bp_test2
	rm -f *db
