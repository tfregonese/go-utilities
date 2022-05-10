# file-processor

This script is a generic file processor. It cannot run by itself. It must be instanciated by a specific script.
Two interfaces are exposed.  

The first one is the processor itself. It must not be nil. 
```
type Processor interface {
	Validate([]string) error
	GetIdentifier(Input) uint64
	GetIdentifierDescription() string
	Process(Input) Output
}
```
The second interface is a token processor. It can be nil. If it is not nil, then the script validates that a token
is provided as a program argument and the interface's function is called before the `Processor.Process(Input)` call.
```
type TokenProcessor interface {
	//SetToken sets an access token
	SetToken(string)
}
```

## Usage

There's an usage example where indexer is a type that implements Processor interface.
```
type indexer struct {
	token string
}

func main() {
	i := indexer{}
	processor := fileprocessor.MakeFileProcessor(i, nil)
	processor.Process()
}
```

The scripts arguments for its execution are,

| name                             | required           | default-value              |
| -------------------------------- | ------------------ | -------------------------- |
| inputPath                        | yes                | -                          |
| outputPath                       | yes                | -                          |
| threads                          | no                 | 25                         |
| hasHeader                        | no                 | true                       |
| token                            | no                 | -                          |
| showDescription                  | no                 | false                      |

Bare in mind that by default the script assumes there's a header in the input file. That means that the first line 
is skipped. If the input file has no header, then the hasHeader argument should be provided with a false value. 

In order to not to skip the first line he argument should be `-hasHeader=false`

## Output

It produces an output in the provided output path and its content is the same as the input content plus a column
that stores the failure message for each failed processed line.

For performance optimization the output file writes are buffered. It writes to the file once for every 100 elements 
processed (successes and failures).

## Changelog

### 0.0.1 - 2020-10-26

#### Added
- Initial generic script for file processing
