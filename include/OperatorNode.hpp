#ifndef OPERATOR_NODE_HPP
#define OPERATOR_NODE_HPP

#include <string>
#include <deque>
#include <unordered_map>
#include <tuple>
#include <math.h>
#include <iostream>

using std::string;
using std::deque;
using std::unordered_map;
using std::pair;

enum type {
	STRING, INT, DATE
};

//global variable D:
extern int _block_size;
extern int _nBuf;

class Table {
	public:
		Table(string name, unsigned int tuple_quantity);
		//Table(const Table& to_copy);
		virtual ~Table();

		unsigned int best_access_cost() const;

		//must be unique
		void add_attribute(string attribute_name, type attribute_type, unsigned int size, unsigned int variability);
		//must be valid attributes already in the table
		void add_primary_key(deque<string> primary_key);
		void add_foreign_key(string attribute_name, string foreign_table_name);
		void add_secondary_index(string attribute_name, unsigned int n, unsigned int fi);
		void add_secondary_hash_index(string attribute_name)
			{_secondary_indexes. insert(std::pair<string, std::pair<unsigned int, unsigned int>>(attribute_name, std::pair<unsigned int, unsigned int>(0 ,1)));}
		void add_primary_index(string attribute_name, unsigned int n, unsigned int fi);
		void ordered_by(string attribute);

		string name() const {return _name;};

		virtual int tuple_quantity() const;
		int block_quantity() const {return ceil((double)_tuple_quantity / (double)block_factor());};

		int block_factor() const {return floor(_block_size / size());};
		int size() const;
		pair<unsigned int, unsigned int> primary_index(string attribute_name) const;
		pair<unsigned int, unsigned int> secondary_index(string attribute_name) const;
		unsigned int primary_index_access_cost(string attribute_name) const;
		unsigned int secondary_index_access_cost(string attribute_name) const;
		double attribute_cardinality(string attribute_name) const {return _tuple_quantity/std::get<2>(_attributes.at(attribute_name));};
		deque<string> primary_key() const {return _primary_key;};
        unordered_map<string, std::tuple<type, unsigned int, unsigned int>> get_attributes() const {return _attributes;};
        unordered_map<string, string> get_fks() const {return _foreign_keys;};
        string ordered_by() const {return _ordered_by;};
        virtual int total_access_cost() const { return 0;};

	protected:
		string _name;
		//attribute name indexes type, size in bytes and variability
		unordered_map<string, std::tuple<type, unsigned int, unsigned int>> _attributes;
		//must contain members of attributes
		deque<string> _primary_key;
		//tuple quantity
		unsigned int _tuple_quantity;
		//foreign keys. attribute name indexes table name
		unordered_map<string, string> _foreign_keys;
		//Does this table have a primary index? What is its N and Fi? If not, <0,0>. <0,1> indicates a hash index
		unordered_map<string, std::pair<unsigned int, unsigned int>> _primary_indexes;
		//secondary index<name, N, Fi>
		unordered_map<string, std::pair<unsigned int, unsigned int>> _secondary_indexes;
		//are the tuples ordered in disk by an attribute? If not, empty string
		string _ordered_by;

};

class Expression {
	public:
		Expression();
		virtual ~Expression();
		virtual int tuple_quantity(const Table * table) const = 0;
		virtual int best_access_cost(const Table * table) const = 0;
};

class AndExpression : public Expression {
	public:
		AndExpression(const Expression* left, const Expression* right);

		int tuple_quantity(const Table * table) const;
		int best_access_cost(const Table * table) const;
	private:
		const Expression *_left, *_right;
};

class OrExpression : public Expression {
	public:
		OrExpression(const Expression* left, const Expression* right);

		int tuple_quantity(const Table * table) const;
		int best_access_cost(const Table * table) const;
	private:
		const Expression *_left, *_right;
};

class FinalExpression {
	public:
		FinalExpression(const std::pair<string, string> left, const std::pair<string, string> right) :
			_left_attribute(left), _right_attribute(right)
		{};
		virtual ~FinalExpression() {};

		virtual int best_access_cost(const Table * table) const;
	protected:
		//contains <Table_name, attribute_name>
		//or just <"", value> for literals
		const std::pair<string, string> _left_attribute, _right_attribute;
};

class EqualExpression: public Expression, public FinalExpression {
	public:
		EqualExpression(const std::pair<string, string> left, const std::pair<string, string> right);

		int tuple_quantity(const Table * table) const;
		int best_access_cost(const Table * table) const;
};

class NotEqualExpression : public Expression, public FinalExpression {
	public:
		NotEqualExpression(const std::pair<string, string> left, const std::pair<string, string> right);

		int tuple_quantity(const Table * table) const;
		int best_access_cost(const Table * table) const;
};

//The attribute must be on the left and the literal on the right!
class GreaterExpression : public Expression, public FinalExpression {
	public:
		GreaterExpression(const std::pair<string, string> left, const std::pair<string, string> right);

		int tuple_quantity(const Table * table) const;
		int best_access_cost(const Table * table) const;
};

//The attribute must be on the left and the literal on the right!
class LessExpression : public Expression, public FinalExpression {
	public:
		LessExpression(const std::pair<string, string> left, const std::pair<string, string> right);

		int tuple_quantity(const Table * table) const;
		int best_access_cost(const Table * table) const;
};

class SelectionNode : public Table {

	public:
		SelectionNode(const Table* left, const Expression* expression);
		virtual ~SelectionNode();

        int tuple_quantity() const;
		int best_access_cost() const;
        int total_access_cost() const {return _child->total_access_cost() + best_access_cost();};

	private:
		const Table *_child;
		//tree containing the expression
		const Expression* _expression;
};

class ProjectionNode : public Table {

	public:
		ProjectionNode(const Table* left, deque<std::pair<string, string>> attributes);
		virtual ~ProjectionNode();

		int best_access_cost() const;
        int total_access_cost() const {return _child->total_access_cost() + best_access_cost();};

	private:
		const Table *_child;
		//tablename e attribute name
		deque<std::pair<string, string>> _attribs;

};

class ProductNode : public Table {

	public:
		ProductNode(const Table* left, const Table* right);
		virtual ~ProductNode();

		int best_access_cost() const;
        int total_access_cost() const {return _left->total_access_cost() + _right->total_access_cost() + best_access_cost();};

	private:
		const Table *_left, *_right; //children
};

class JoinNode : public Table {

	public:
		JoinNode(const Table* left, const Table* right, const Expression* expression);
		virtual ~JoinNode();

		int best_access_cost() const;
        int total_access_cost() const {return _left->total_access_cost() + _right->total_access_cost() + best_access_cost();};

	private:
		const Table *_left, *_right; //children
		const Expression* _expression;
        int A1() const;
        int A2() const;
        int A3() const;
        int A4() const;
};

class NaturalJoinNode : public Table {

	public:
		NaturalJoinNode(const Table* left, const Table* right);
		virtual ~NaturalJoinNode();

		int best_access_cost() const;
        int total_access_cost() const {return _left->total_access_cost() + _right->total_access_cost() + best_access_cost();};

	private:
		const Table *_left, *_right; //children
		int A1() const;
		int A2() const;
		int A3() const;
		int A4() const;
};

#endif  // OPERATOR_NODE_HPP
