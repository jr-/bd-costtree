#ifndef BINARY_NODE_HPP
#define BINARY_NODE_HPP

#include <cstdlib>
#include <vector>
#include <stdexcept>
#include <string>
using namespace std;

class BinaryNode {
 public:
  enum NodeType{SELECTION, PROJECTION, PRODUCT, JOIN, NATURAL, TABLE};
 protected:
	string _exp;
  NodeType _type;
  int _cost = -1;
  int _nTuplas = -1;
	BinaryNode* _left;
	BinaryNode* _right;

 public:
	BinaryNode(string expr, NodeType type) {
		_left = nullptr;
		_right = nullptr;
		_exp = expr;
    _type = type;
	}

	string getExp() {
		return _exp;
	}

  NodeType getType() {
    return _type;
  }

  int getCost() {
    return _cost;
  }

  int getNTuplas() {
    return _nTuplas;
  }

	BinaryNode* getLeft() {
		return _left;
	}

	BinaryNode* getRight() {
		return _right;
	}

  int calcNTuplas() {
    return -1;
  }

  int calcTempoAcesso() {
    return -1;
  }

	int insert(string expr, NodeType type) {
		if(_type == NodeType::PROJECTION || _type == NodeType::SELECTION) {
			if(_left == nullptr) {
				_left = new BinaryNode(expr, type);
        return 0;
			} else {
				return -1;
			}
		} else if(_type == NodeType::TABLE) {
      return -1;
		} else {
      if(_left == nullptr) {
        _left = new BinaryNode(expr, type);
        return 0;
      } else {
        if(_right == nullptr) {
          _right = new BinaryNode(expr, type);
          return 0;
        } else {
          return 1; //exception
        }
      }
    }
	}
};

#endif  // BINARY_NODE_HPP
