all:default
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $*.java
        
CLASSES = \
        FileInterface.java \
        Proxy.java \
        Server.java \

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class