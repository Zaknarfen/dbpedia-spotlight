#!/bin/bash
#+------------------------------------------------------------------------------------------------------------------------------+
#| DBpedia Spotlight - Create database-backed model                                                                             |
#| @author Joachim Daiber                                                                                                       |
#+------------------------------------------------------------------------------------------------------------------------------+

# $1 Data directory
# $2 Spotlight installation directory
# $3 Locale (en_US)
# $3 Stopwords file
# $4 Analyzer+Stemmer language prefix e.g. Dutch(Analzyer|Stemmer)
# $5 Model target folder

#+------------------------------------------------------------------------------------------------------------------------------+
#| Functions                                                                                                                    |
#+------------------------------------------------------------------------------------------------------------------------------+

usage()
{
     echo "index_db.sh"
     echo "Parameters: "
     echo "1) Data directory (example /home/Spotlight/data)"
     echo "2) Spotlight installation directory (example /home/ubuntu/dbpedia-spotlight)"
     echo "3) Locale (example en_US)"
     echo "4) LanguagePrefix(Analyzer|Stemmer) (example DutchStemmer)"
     echo "5) Model target folder (example finalmodel)"
     echo " "
     echo "Usage: ./index_db.sh /home/Spotlight/data /home/ubuntu/dbpedia-spotlight nl_NL DutchStemmer final_model"
     echo "Create a database-backed model of DBpedia Spotlight for a specified language."
     echo " "
}

setUpSpotlight()
{
    echo "Testing the DBpedia Spotlight installation..."
    if [ -d $1 ]; then
        echo "DBpedia Spotlight is already installed"
    else
        echo "No installation detected. Setting up DBpedia Spotlight..."
        echo "Creating the DBpedia Spotlight base directory, cloning the repository and installing..."
        create_dir $1
        cd $1
        #git clone --depth 1 https://github.com/dbpedia-spotlight/dbpedia-spotlight.git
        #mvn -T 1C -q clean install
        echo "Done!"
    fi
}

fixPom()
{
    #Split the original pom.xml to tmp files so we can reconstruct them
    awk '{if (NR<=134) {print $0}}' pom.xml > pom_tmp1.xml
    awk '{if (NR>=142 && NR<=147) {print $0}}' pom.xml > pom_tmp2.xml
    awk '{if (NR>=135 && NR<=141) {print $0}}' pom.xml > pom_tmp3.xml
    awk '{if (NR>=148) {print $0}}' pom.xml > pom_tmp4.xml

    #Reconstruct the pom.xml
    awk '{{print $0}}' pom_tmp1.xml > pom.xml
    awk '{{print $0}}' pom_tmp2.xml >> pom.xml
    awk '{{print $0}}' pom_tmp3.xml >> pom.xml
    awk '{{print $0}}' pom_tmp4.xml >> pom.xml

}

setUpPigNLProc()
{
    if [ -d $1 ]; then
        echo "Updating PigNLProc..."
        cd $1/pignlproc
        echo "$(pwd)"
        git reset --hard HEAD
        git pull
    else
        echo "Setting up PigNLProc..."
        #mkdir -p $1
        cd $1
        #git clone --depth 1 https://github.com/dbpedia-spotlight/pignlproc.git
        git clone https://github.com/ogrisel/pignlproc.git
        cd pignlproc
        echo "$(pwd)"
        echo "Building PigNLProc..."
    fi
    #first approach
    #mvn package -Dmaven.test.skip=true
    #second approach
    fixPom
    mvn -T 1C -q assembly:single -Dmaven.test.skip=true
}

loadDumpToHDFS()
{
    hadoop fs -put $1/${2}wiki-latest-pages-articles.xml ${2}wiki-latest-pages-articles.xml
    #$1/${2}wiki-latest-pages-articles.xml.bz2" | bzcat | hadoop fs -put - ${2}wiki-latest-pages-articles.xml
    #echo "$eval"
    #if [ "$eval" == "" ]; then
    #    $1/${2}wiki-latest-pages-articles.xml.bz2" | bzcat | hadoop fs -put - ${2}wiki-latest-pages-articles.xml
    #else
    #    $1/${2}wiki-latest-pages-articles.xml.bz2" | bzcat | python $4/pignlproc/utilities/split_train_test.py 12000 $3/heldout.txt | hadoop fs -put - ${2}wiki-latest-pages-articles.xml
    #fi
}

loadStopwordsToHDFS()
{
    hadoop fs -put $1 stopwords.list

    if [ -e "$2/$LANGUAGE-token.bin" ]; then
        hadoop fs -put "$2/$3-token.bin" "$3.tokenizer_model"
    else
        touch empty;
        hadoop fs -put empty "$3.tokenizer_model";
        rm empty;
    fi
}

runPig()
{
    echo "$1"Analyzer
    echo "$PIGNLPROC_JAR"
    echo "$WIKIPEDIA_DATA"
    echo "$WIKIPEDIA_DATA/${LANGUAGE}wiki-latest-pages-articles.xml"

    pig -param LANG="$LANGUAGE" \
        -param ANALYZER_NAME="$1Analyzer" \
        -param INPUT="hadoop-data/data/${LANGUAGE}wiki-latest-pages-articles.xml" \
        -param OUTPUT_DIR="/usr/local/pt/tokenCounts" \
        -param STOPLIST_PATH="/data/stopwords.list" \
        -param STOPLIST_NAME="stopwords.list" \
        -param PIGNLPROC_JAR="SpotlightTest/data/pig/pignlproc/target/pignlproc-0.1.0-SNAPSHOT.jar" \
        -param MACROS_DIR="SpotlightTest/data/pig/pignlproc/examples/macros/" \
        -m SpotlightTest/data/pig/pignlproc/examples/indexing/token_counts.pig.params SpotlightTest/data/pig/pignlproc/examples/indexing/token_counts.pig

    pig -param LANG="$LANGUAGE" \
        -param LOCALE="$LOCALE" \
        -param INPUT="/hadoop-data/data/${LANGUAGE}wiki-latest-pages-articles.xml" \
        -param OUTPUT="/usr/local/pt/names_and_entities" \
        -param TEMPORARY_SF_LOCATION="$TARGET_DIR/sf_lookup" \
        -param PIGNLPROC_JAR="/cygwin/usr/local/SpotlightTest/data/pig/pignlproc/target/pignlproc-0.1.0-SNAPSHOT.jar" \
        -param MACROS_DIR="/cygwin/usr/local/SpotlightTest/data/pig/pignlproc/examples/macros/" \
        -m SpotlightTest/data/pig/pignlproc/examples/indexing/names_and_entities.pig.params SpotlightTest/data/pig/pignlproc/examples/indexing/names_and_entities.pig

    exit
}

copyResult()
{
    hadoop fs -cat $1* > $2
}

createSpotlightModel()
{
    mvn -pl index exec:java -Dexec.mainClass=org.dbpedia.spotlight.db.CreateSpotlightModel -Dexec.args="$1 $DBPEDIA_DATA $TARGET_DIR $OPENNLP_DATA $STOPWORDS PortugueseStemmer";

    if [ "$eval" == "true" ]; then
        mvn -pl eval exec:java -Dexec.mainClass=org.dbpedia.spotlight.evaluation.EvaluateSpotlightModel -Dexec.args="$TARGET_DIR $RESOURCES_DATA/heldout.txt" > $TARGET_DIR/evaluation.txt
    fi
}

indexDbMain()
{
    #Set up Spotlight:
    setUpSpotlight $SPOTLIGHT_WORKSPACE

    #Test if the data directory exists, if not run the download script
    if [ ! -d $DATA_DIR ]; then
        echo "Running the download.sh script now..."
        downloadMain $DATA_DIR
        echo "Done!"
    fi

    #Set up pig:
    #setUpPigNLProc $PIG_DATA

    #cd E:/hadoop-1.2.1/bin

    #Load the dump into HDFS:
    #cd E:/hadoop-data/data
    #cd /hadoop-data/data
    echo $(pwd)
    #echo "Loading Wikipedia dump into HDFS..."
    #loadDumpToHDFS $WIKIPEDIA_DATA $LANGUAGE $RESOURCES_DATA $PIG_DATA

    OPENNLP_DATA=$DATA_DIR/opennlp/$LANGUAGE
    STOPWORDS=$DATA_DIR/resources/$LANGUAGE/stopwords.list

    #Load the stopwords into HDFS:
    #echo "Loading stopwords into HDFS..."
    #cd $DATA_DIR
    #loadStopwordsToHDFS $STOPWORDS $OPENNLP_DATA $LANGUAGE

    #hadoop fs -put localfile1 localfile2 /user/hadoop/hadoopdir

    #Adapt pig params:
    #cd $DATA_DIR/pig/pignlproc
    cd /usr/local
    echo $(pwd)

    #PIGNLPROC_JAR="$PIG_DATA/pignlproc/target/pignlproc-0.1.0-SNAPSHOT.jar"
    PIGNLPROC_JAR="target/pignlproc-0.1.0-SNAPSHOT.jar"

    #Run pig:
    runPig $STEMMER
    exit

    #Copy results to local:
    cd $DBPEDIA_DATA
    copyResult $TARGET_DIR/tokenCounts/part tokenCounts
    copyResult $TARGET_DIR/names_and_entities/pairCounts/part pairCounts
    copyResult $TARGET_DIR/names_and_entities/uriCounts/part uriCounts
    copyResult $TARGET_DIR/names_and_entities/sfAndTotalCounts/part sfAndTotalCounts

    #Create the model:
    cd $SPOTLIGHT_WORKSPACE
    echo $(pwd)
    #exit
    createSpotlightModel $LOCALE $STEMMER

    echo "Finished!"
}

#+------------------------------------------------------------------------------------------------------------------------------+
#| Main                                                                                                                         |
#+------------------------------------------------------------------------------------------------------------------------------+

export MVN_OPTS="-Xmx10G"

eval=""

while getopts "e:" opt; do
    case $opt in
        e) eval="true";;
    esac
done

if ([ $# == 6 ] && [ eval == "true" ])
then
    shift $((OPTIND - 1))
fi

if ([ $# != 5 ] && [ $# != 6])
then
    usage
    exit
fi

# Paths to all the directories we are going to need
SPOTLIGHT_WORKSPACE=$2
LANGUAGE=$(echo $3 | sed "s/_.*//g")

#Including the other script so we can use its functions
source $(dirname $0)/download.sh $SPOTLIGHT_WORKSPACE $LANGUAGE

DATA_DIR=$1
SPOTLIGHT_WORKSPACE=$2
LOCALE=$3
STEMMER=$4
LANGUAGE=$(echo $3 | sed "s/_.*//g")
OUTPUT_DIR=$DATA_DIR/output/$LANGUAGE
RESOURCES_DATA=$DATA_DIR/resources/$LANGUAGE
WIKIPEDIA_DATA=$DATA_DIR/wikipedia/$LANGUAGE
TARGET_DIR=$OUTPUT_DIR/finalmodel
PIG_DATA=$DATA_DIR/pig
DBPEDIA_DATA=$DATA_DIR/dbpedia/$LANGUAGE
OPENNLP_DATA=""
STOPWORDS=""

# Stop processing if one step fails
set -e

#Call the main routine
indexDbMain

set +e

