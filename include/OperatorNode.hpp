#ifndef OPERATOR_NODE_HPP
#define OPERATOR_NODE_HPP

#include <string>
#include <deque>

using std::string;
using std::deque;

enum type {
	STRING, INT
};

class Expression {};
class AndExpression : public Expression {};
class OrExpression : public Expression {};
class EqualExpression: public Expression {};
class NotEqualExpression : public Expression {};
class GreaterExpression : public Expression {};
class LessExpression : public Expression {};

class Table;

class OperatorNode {

	public:
		OperatorNode();
		virtual ~OperatorNode();

		int best_access_number();
		Table result();

	protected:
		const OperatorNode* _left, *_right;
};

class Table : public OperatorNode {
	public:
		Table(string name, deque<std::tuple<string, type, int>> attributes, deque<string> primary_key, deque<std::tuple<string, string>> foreign_keys, std::pair<int, int>* _primary_index, deque<std::tuple<string, int, int>> secondary_index, int tuple_quantity);
		virtual ~Table();

		Table result();
		int best_access_number();

	private:
		string _name;
		//attribute name, type and size in bytes
		deque<std::tuple<string, type, int>> _attributes;
		//must contain members of attributes
		deque<string>_primary_key;
		//foreign keys<attribute name, table name>
		deque<std::tuple<string, string>> _foreign_keys;
		//Does this table have a primary index? What is its N and Fi?
		std::pair<int, int>* _primary_index_values;
		//secondary index<name, N, Fi>
		deque<std::tuple<string, int, int>> _secondary_index;
		//tuple quantity
		int _tuple_quantity;

};

class SelectionNode : OperatorNode {

	public:
		SelectionNode(const OperatorNode* left, const OperatorNode* right, Expression expression);
		virtual ~SelectionNode();

		int best_access_number();
		Table result();

	private:
		//tree containing the expression
		Expression _expression;

};

class ProjectionNode : OperatorNode {

	public:
		ProjectionNode(const OperatorNode* left, const OperatorNode* right, deque<string, string> attributes);
		virtual ~ProjectionNode();

		int best_access_number();
		Table result();

	private:
		//attributes<attr name, table name> attributes to project
		deque<string, string> _attributes;

};

class ProductNode : OperatorNode {

	public:
		ProductNode(const OperatorNode* left, const OperatorNode* right);
		virtual ~ProductNode();

		int best_access_number();
		Table result();

};

class JoinNode : OperatorNode {

	public:
		JoinNode(const OperatorNode* left, const OperatorNode* right, Expression expression);
		virtual ~JoinNode();

		int best_access_number();
		Table result();

	private:
		Expression _expression;

};

class NaturalJoinNode : OperatorNode {

	public:
		NaturalJoinNode(const OperatorNode* left, const OperatorNode* right);
		virtual ~NaturalJoinNode();

		int best_access_number();
		Table result();

};

//selection, projection, product, join, naturalJoin

#endif  // OPERATOR_NODE_HPP
