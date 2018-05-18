HADOOP_PATH = ${hadoop} classpath

H = HadoopSort

hadoopsort8GB:
	javac -classpath $$(hadoop classpath)  $(H).java 
	jar -cvf $(H).jar *.class
	sbatch hadoopsort8GB.slurm

hadoopsort20GB:
	javac -classpath $$(hadoop classpath)  $(H).java 
	jar -cvf $(H).jar *.class
	sbatch hadoopsort20GB.slurm

hadoopsort80GB:
	javac -classpath $$(hadoop classpath)  $(H).java 
	jar -cvf $(H).jar *.class
	sbatch hadoopsort80GB.slurm

clean: 
	rm -rf *.jar
