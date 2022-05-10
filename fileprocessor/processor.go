package fileprocessor

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

const (
	defaultRoutines = 25
)

type Processor interface {
	//Validate validates whether the current line is valid or not
	Validate([]string) error
	//GetIdentifier returns the line identifier description and id for the given Input
	GetIdentifier(Input) (string, uint64)
	//Process does some processing for the given Input and returns a resulting Output
	Process(Input) Output
	//SetToken sets an access token
	SetToken(string)
}

type Input struct {
	Line []string
}

type Output struct {
	Line    []string
	Error   error
	Success bool
}

type result struct {
	Input  Input
	Output Output
}

type fileProcessor struct {
	inputs    chan Input
	results   chan result
	processor Processor
}

func Process(processor Processor) {
	if processor == nil {
		log.Fatal("processor cannot be nil")
	}

	fProcessor := fileProcessor{
		inputs:    make(chan Input, 100),
		results:   make(chan result, 100),
		processor: processor,
	}

	fProcessor.run()
}

func (p fileProcessor) run() {
	var inputPathArg = "inputPath"
	var outputPathArg = "outputPath"
	var tokenArg = "token"
	inputPathPtr := flag.String(inputPathArg, "default input", "input file path")
	outputPathPtr := flag.String(outputPathArg, "default output", "output file path")
	routinesNumberPtr := flag.Int("threads", defaultRoutines, "number of parallel executions")
	hasHeaderPtr := flag.Bool("hasHeader", true, "indicates if the input file has a header or not, true by default")
	token := flag.String(tokenArg, "", "access token")
	showDescription := flag.Bool("showDescription", false, "is description shown")

	requiredArguments := []string{inputPathArg, outputPathArg}
	if p.processor != nil {
		requiredArguments = append(requiredArguments, tokenArg)
	}
	flag.Parse()

	seen := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	for _, req := range requiredArguments {
		if !seen[req] {
			fmt.Fprintf(os.Stderr, "missing requiredArguments -%s argument\n", req)
			os.Exit(2) // the same exit code flag.Parse uses
		}
	}

	if p.processor != nil {
		p.processor.SetToken(*token)
	}

	inputFile, err := os.Open(*inputPathPtr)
	if err != nil {
		log.Fatalf("error opening input file %v", err)
	}

	outputFile, err := os.Create(*outputPathPtr)
	if err != nil {
		log.Fatal("error creating output file")
	}

	fmt.Println("---------------------------------------------------------------")
	fmt.Println("Process started")
	fmt.Println("---------------------------------------------------------------")
	fmt.Printf("input file path: %s\n", *inputPathPtr)
	fmt.Printf("output file path: %s\n", *outputPathPtr)
	fmt.Printf("number of parallel executions: %d\n", *routinesNumberPtr)
	fmt.Printf("header presence: %t\n", *hasHeaderPtr)
	if *token != "" {
		fmt.Printf("token: %s\n", *token)
	}
	fmt.Printf("---------------------------------------------------------------")
	fmt.Printf("\n\n\n\n")

	//Success Writer:
	successWriter := csv.NewWriter(outputFile)
	defer successWriter.Flush()

	//Failure Writer:
	failuresFile, err := os.Create("failures.csv")
	if err != nil {
		log.Fatal("error creating output file")
	}
	failureWriter := csv.NewWriter(failuresFile)
	defer failureWriter.Flush()

	// Create a new reader.
	reader := csv.NewReader(bufio.NewReader(inputFile))
	if *hasHeaderPtr {
		header, err := reader.Read()
		if err != nil {
			log.Fatal("error reading header from input file")
		}

		err = successWriter.Write(append(header))
		if err != nil {
			log.Fatal("error writing header to output file")
		}

		if *showDescription {
			err = failureWriter.Write(append(header, "error_description"))
		} else {
			err = failureWriter.Write(append(header))
		}
		if err != nil {
			log.Fatal("error writing header to output file")
		}
	}

	var successCounter int64
	var failureCounter int64
	var totalCounter int64
	routinesNumber := *routinesNumberPtr
	start := time.Now()

	group := sync.WaitGroup{}
	group.Add(routinesNumber)
	for w := 1; w <= routinesNumber; w++ {
		go p.worker(w, &group)
	}

	go func() {
		group.Wait()
		close(p.results)
	}()

	go p.readFile(reader)
	count := 0
	fmt.Println("starting to wait for results")
	for record := range p.results {
		count++

		var outLine []string

		if record.Output.Success {
			outLine = append(record.Input.Line)
			err = successWriter.Write(outLine)
			if err != nil {
				_, id := p.processor.GetIdentifier(record.Input)
				fmt.Println(fmt.Sprintf("error writting item to output with id: %d", id))
			}
			successCounter++
		} else if record.Output.Error != nil {
			if *showDescription {
				outLine = append(record.Input.Line, record.Output.Error.Error())
			} else {
				outLine = append(record.Input.Line)
			}
			err = failureWriter.Write(outLine)
			if err != nil {
				_, id := p.processor.GetIdentifier(record.Input)
				fmt.Println(fmt.Sprintf("error writting item to output with id: %d", id))
			}
			failureCounter++
		}

		if count%100 == 0 {
			successWriter.Flush()
			failureWriter.Flush()
		}

		totalCounter++

		desc, id := p.processor.GetIdentifier(record.Input)
		fmt.Printf(" %d processed. failure: %t\t%s: %d\n", count, record.Output.Error != nil, desc, id)
	}

	end := time.Now()
	fmt.Println(fmt.Sprintf("Total: %d", totalCounter))
	fmt.Println(fmt.Sprintf("Succeded inputs: %d", successCounter))
	fmt.Println(fmt.Sprintf("Failed: %d", failureCounter))
	fmt.Printf("Took %v to run.\n", end.Sub(start))
}

func (p fileProcessor) worker(id int, group *sync.WaitGroup) {
	fmt.Println("worker ", id, " started")
	defer func() {
		group.Done()
	}()
	for input := range p.inputs {
		output := p.processor.Process(input)

		result := result{
			Input:  input,
			Output: output,
		}
		p.results <- result
	}
}

func (p fileProcessor) readFile(reader *csv.Reader) {
	fmt.Println("start reading file")
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		err = p.processor.Validate(line)
		if err != nil {
			log.Fatalf("error reading Line %v with error %p", line, err)
		}

		p.inputs <- Input{Line: line}
	}
	close(p.inputs)
}
