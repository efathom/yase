package chunking

import (
	"context"

	sitter "github.com/smacker/go-tree-sitter"
)

// ASTChunker splits source code into function/method/class-level chunks using
// tree-sitter AST parsing. It preserves scope integrity by never splitting
// inside a function body.
func ASTChunker(code []byte, lang *sitter.Language) []string {
	if lang == nil || len(code) == 0 {
		return nil
	}

	parser := sitter.NewParser()
	parser.SetLanguage(lang)
	tree, err := parser.ParseCtx(context.Background(), nil, code)
	if err != nil || tree == nil {
		return nil
	}
	root := tree.RootNode()
	if root == nil {
		return nil
	}

	var chunks []string
	var traverse func(node *sitter.Node)

	traverse = func(node *sitter.Node) {
		t := node.Type()
		// Semantic boundaries: functions, methods, classes
		if t == "function_declaration" || t == "method_declaration" ||
			t == "function_definition" || t == "class_definition" ||
			t == "class_declaration" {
			chunk := string(code[node.StartByte():node.EndByte()])
			chunks = append(chunks, chunk)
			return // Do not recurse inside — preserve scope integrity
		}
		for i := uint32(0); i < node.ChildCount(); i++ {
			traverse(node.Child(int(i)))
		}
	}
	traverse(root)
	return chunks
}
