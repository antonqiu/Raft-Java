all:
ifeq ($(OS),Windows_NT)
	# assume windows
	javac -Xlint -g *.java
else
	# assume Linux
	javac -Xlint -g *.java
endif

clean:
	rm *.class
	rm lib/*.class
