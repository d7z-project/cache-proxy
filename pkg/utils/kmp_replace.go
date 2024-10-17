package utils

import (
	"io"
)

// KMP算法实现 (当前使用 ChatGPT 4o 生成)
func kmpSearch(text, pattern []byte) []int {
	n := len(text)
	m := len(pattern)

	// 计算前缀表
	lps := make([]int, m)
	j := 0 // j是模式字符串的索引
	computeLPSArray(pattern, m, lps)

	i := 0 // i是文本字符串的索引
	var result []int
	for i < n {
		if pattern[j] == text[i] {
			i++
			j++
		}
		if j == m {
			result = append(result, i-j) // 找到匹配，记录起始索引
			j = lps[j-1]
		} else if i < n && pattern[j] != text[i] {
			if j != 0 {
				j = lps[j-1]
			} else {
				i++
			}
		}
	}
	return result
}

// 计算前缀表
func computeLPSArray(pattern []byte, m int, lps []int) {
	length := 0
	lps[0] = 0
	i := 1

	for i < m {
		if pattern[i] == pattern[length] {
			length++
			lps[i] = length
			i++
		} else {
			if length != 0 {
				length = lps[length-1]
			} else {
				lps[i] = 0
				i++
			}
		}
	}
}

func NewKMPReplaceReader(reader io.ReadCloser, src []byte, dest []byte) io.ReadCloser {
	return &KMPReplaceReader{
		reader:  reader,
		pattern: src,
		repl:    dest,
	}
}

type KMPReplaceReader struct {
	reader  io.ReadCloser
	pattern []byte
	repl    []byte
}

func (r *KMPReplaceReader) Close() error {
	return r.reader.Close()
}

func (r *KMPReplaceReader) Read(p []byte) (n int, err error) {
	buf := make([]byte, len(p))
	n, err = r.reader.Read(buf)
	if err != nil {
		return n, err
	}

	// 使用KMP算法查找模式
	matches := kmpSearch(buf[:n], r.pattern)

	// 替换匹配的内容
	var result []byte
	lastIndex := 0
	for _, match := range matches {
		result = append(result, buf[lastIndex:match]...) // 写入匹配前的内容
		result = append(result, r.repl...)               // 写入替换内容
		lastIndex = match + len(r.pattern)               // 更新最后索引
	}
	result = append(result, buf[lastIndex:n]...) // 写入剩余内容

	// 将结果写入输出缓冲区
	copy(p, result)
	return len(result), nil
}
