These files are copied from https://github.com/apache/parquet-testing and the json equivalent is
generated using parquet-tools.jar

To regenerate these files again:

```shell script
mkdir temp

# On a mac you can `brew install parquet-tools`
# These steps are for installing parquet-tools using Linux and require
# java and maven to be installed and on the path 
git clone -b apache-parquet-1.10.0 https://github.com/apache/parquet-mr.git temp/parquet-mr
mvn clean package -Plocal -f temp/parquet-mr/parquet-tools
alias parquet-tools="java -jar $PWD/temp/parquet-mr/parquet-tools/target/parquet-tools-1.10.0.jar"

# Download test files
git clone https://github.com/apache/parquet-testing.git temp/parquet-testing
for PF in temp/parquet-testing/data/*.parquet ; do
  cp $PF ./parquet-testing-$(basename $PF)
done
git clone https://github.com/dask/fastparquet.git temp/fastparquet
for PF in temp/fastparquet/test-data/*.parquet ; do
  cp $PF ./fastparquet-$(basename $PF)
done

# Generate output we can compare to in our tests
for PF in *.parquet ; do
   parquet-tools cat -j --no-color $PF >$PF.json 
done

# Once you are happy, clean up the extra folder
rm -r temp
``` 
