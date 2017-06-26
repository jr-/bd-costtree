#include "OperatorNode.hpp"

#include <algorithm>
#include <cmath>
#include <iostream>

using std::string;
using std::deque;
using std::unordered_map;
using std::pair;

/*class Expression {};
class AndExpression : public Expression {};
class OrExpression : public Expression {};
class EqualExpression: public Expression {};
class NotEqualExpression : public Expression {};
class GreaterExpression : public Expression {};
class LessExpression : public Expression {};*/

Table::Table(string name, unsigned int tuple_quantity) :
	_name(name),
	_tuple_quantity(tuple_quantity)
	{}
Table::~Table(){}

/*Table::Table(const Table& to_copy) : _name(to_copy._name), _attributes(to_copy._attributes), _primary_key(to_copy._primary_key),  _tuple_quantity(to_copy._tuple_quantity), _foreign_keys(to_copy._foreign_keys), _primary_index(to_copy._primary_index), _secondary_indexes(to_copy._secondary_indexes), _ordered_by(to_copy._ordered_by)
{}*/

unsigned int Table::best_access_cost() const
{
	//does this make sense for Table? Maybe not
    return 0;
}

void Table::add_attribute(string attribute_name, type attribute_type, unsigned int size, unsigned int variability)
{
	//name must be unique
	auto got = _attributes.find(attribute_name);
	if(got != _attributes.end()) {
		//attribute already exists, cant be inserted
		//TODO should we raise an exception?
		return;
	}
	auto attribute_characteristics = std::tuple<type, unsigned int, unsigned int>(attribute_type, size, variability);
	auto to_add = std::pair<string, std::tuple<type, unsigned int, unsigned int>>(attribute_name, attribute_characteristics);
	_attributes.insert(to_add);
}

void Table::add_primary_key(deque<string> primary_key)
{
	//all the members of primary_key must be attributes
	for(string i : primary_key) {
		if(_attributes.find(i) == _attributes.end()) {
			//primary key must be an attribute!
			continue;
		}
		_primary_key.push_back(i);
	}
}

void Table::add_foreign_key(string attribute_name, string foreign_table_name)
{
	//attribute_name must be an attribute in this table
	if(_attributes.find(attribute_name) == _attributes.end()) {
		//attribute_name is not an attribute of this table
		return;
	}
	_foreign_keys.insert(std::pair<string, string>(attribute_name, foreign_table_name));
}

void Table::add_secondary_index(string attribute_name, unsigned int n, unsigned int fi)
{
	//attribute_name must be the name of an attribute of this table.
	if(_attributes.find(attribute_name) == _attributes.end()) {
		//attribute_name is not an attribute of this table
		return;
	}
	auto values = std::pair<unsigned int, unsigned int>(n, fi);
	_secondary_indexes.insert(std::pair<string, std::pair<unsigned int, unsigned int>>(attribute_name, values));
}

void Table::add_primary_index(string attribute_name, unsigned int n, unsigned int fi)
{
	//attribute_name must be the name of an attribute of this table.
	if(_attributes.find(attribute_name) == _attributes.end()) {
		//attribute_name is not an attribute of this table
		return;
	}
	auto values = std::pair<unsigned int, unsigned int>(n, fi);
	_primary_indexes.insert(std::pair<string, std::pair<unsigned int, unsigned int>>(attribute_name, values));
}

pair<unsigned int, unsigned int> Table::primary_index(string attribute_name) const
{
	if(_primary_indexes.find(attribute_name) == _primary_indexes.end()) {
		return pair<unsigned int, unsigned int>(0, 0);
	}
	return _primary_indexes.at(attribute_name);
}

void Table::ordered_by(string attribute)
{
	//attribute must be an attribute of this table
	if(_attributes.find(attribute) == _attributes.end()) {
		//attribute_name is not an attribute of this table
		return;
	}
	_ordered_by = attribute;
}

Expression::Expression(){}
Expression::~Expression(){}

AndExpression::AndExpression(const Expression* left, const Expression* right) : _left(left), _right(right) {}

int AndExpression::tuple_quantity(const Table* table) const
{
	int nr = table->tuple_quantity();
	double cardinalitites = _left->tuple_quantity(table) * _right->tuple_quantity(table);
	return cardinalitites / nr;
}

int AndExpression::best_access_cost(const Table * table) const
{
	return std::min(_left->best_access_cost(table), _right->best_access_cost(table));
}

OrExpression::OrExpression(const Expression* left, const Expression* right) : _left(left), _right(right) {}

int OrExpression::tuple_quantity(const Table* table) const
{
}

int OrExpression::best_access_cost(const Table * table) const
{
	return _left->best_access_cost(table) + _right->best_access_cost(table);
}

EqualExpression::EqualExpression(const std::pair<string, string> left, const std::pair<string, string> right):  _left_attribute(left), _right_attribute(right) {}

int EqualExpression::tuple_quantity(const Table* table) const
{
	int quantity;
	if(_left_attribute.first == "") {
		quantity = table->attribute_cardinality(_right_attribute.second);
	} else {
		quantity = table->attribute_cardinality(_left_attribute.second);
	}
	return quantity;
}

int EqualExpression::best_access_cost(const Table * table) const
{
	deque<pair<int, string>> results;
	pair<int, string> a1 = pair<int, string>(table->block_quantity(), "A1");
	results.push_back(a1);
	string attribute = _left_attribute.second;
	if(_right_attribute.first != "") {
		attribute = _right_attribute.second;
	}
	if(attribute == table->ordered_by()) { //A2
		int calc_a2 = ceil(log2(table->block_quantity()));
		if(deque<string>{attribute} != table->primary_key()) {
			calc_a2 += ceil(table->attribute_cardinality(attribute)/table->block_factor()) - 1;
		}
		auto a2 = pair<int, string>(calc_a2, "A2");
		results.push_back(a2);
	}
	//A3 e A4
}

NotEqualExpression::NotEqualExpression(const std::pair<string, string> left, const std::pair<string, string> right) : _left_attribute(left), _right_attribute(right) {}

int NotEqualExpression::tuple_quantity(const Table* table) const
{
	int quantity;
	if(_left_attribute.first == "") {
		quantity = table->attribute_cardinality(_right_attribute.second);
	} else {
		quantity = table->attribute_cardinality(_left_attribute.second);
	}
	return table->tuple_quantity() - quantity * table->tuple_quantity();
}

int NotEqualExpression::best_access_cost(const Table * table) const
{
}

GreaterExpression::GreaterExpression(const std::pair<string, string> left, const std::pair<string, string> right) : _left_attribute(left), _right_attribute(right) {}

int GreaterExpression::tuple_quantity(const Table* table) const
{
	return table->tuple_quantity() / 2;
}

int GreaterExpression::best_access_cost(const Table * table) const
{
}

LessExpression::LessExpression(const std::pair<string, string> left, const std::pair<string, string> right) : _left_attribute(left), _right_attribute(right) {}

int LessExpression::tuple_quantity(const Table* table) const
{
	return table->tuple_quantity() / 2;
}

int LessExpression::best_access_cost(const Table * table) const
{
}

NaturalJoinNode::NaturalJoinNode(const Table* left, const Table* right) : Table("NaturalJoin" + left->name() + right->name(), left->tuple_quantity() * right->tuple_quantity()), _left(left), _right(right)
{
    //TODO calcular tuple_quantity e adicionar atributos
    //if atributos iguais concatena com o nome da esquerda
    //juncao natural sem atributo em comum nr * ns
    //juncao por referencia fk(R) = pk(S)
    //juncao entre chaves candidatas (atributos unique) ie cpf nas duas tabelas
    //juncao por igualdade (atributos nao chave)
    deque<string> j_attr;
    unordered_map<string, std::tuple<type, unsigned int, unsigned int>> lat = _left->get_attributes();
    unordered_map<string, std::tuple<type, unsigned int, unsigned int>> rat = _right->get_attributes();
    unordered_map<string, std::tuple<type, unsigned int, unsigned int>>::const_iterator got;
    bool l_ord, r_ord = false;
    int result;
    //pega os atributos com o mesmo nome para a juncao
    for(auto &at : lat) {
        got = rat.find(at.first);
        if(got != lat.end()) {
            j_attr.push_back(at.first);
        }
    }
}

NaturalJoinNode::~NaturalJoinNode(){}

int NaturalJoinNode::best_access_cost() const
{
	//we need to log all these results
	int res = A1();
	string best = "A1";
	int a2 = A2();
	if(a2 != 0 && a2 < res) {
		res = a2;
		best = "A2";
	}
	int a3 = A3();
	if(a3 != 0 && a3 < res) {
		res = a3;
		best = "A3";
	}

	int a4 = A4();
	if(a4 != 0 && a4 < res) {
		res = a4;
		best = "A4";
	}
	return res;
}

int NaturalJoinNode::A1() const
{
	//is always possible
	int mult = _left->block_quantity() * _right->block_quantity();
	int res = std::min<int>(_left->block_quantity() + mult, _right->block_quantity() + mult);
	return res;
}

//nao precisa ser necessariamnete indice primario
int NaturalJoinNode::A2() const
{
	//when a2 cant be calculated, return 0
	/*if(!_left->has_primary_index() && !_right->has_primary_index()){
		return 0;
	}
	const Table* indexed, *no_index;
	if(_left->has_primary_index()){
		indexed = _left;
		no_index = _right;
	} else {
		indexed = _right;
		no_index = _left;
	}
	int res = no_index->block_quantity();
	res += no_index->tuple_quantity() * indexed->primary_index_access_cost();
	return res;*/
	return 0;
}
//Se R e S estiverem fisicamente ordenadas pelos atributos de juncao
//Ideia Geral: pega os atributos em comum e verifica se as duas tabelas estão ordenadas fisicamentes pelos atributos em comum
int NaturalJoinNode::A3() const
{
    deque<string> j_attr;
    unordered_map<string, std::tuple<type, unsigned int, unsigned int>> lat = _left->get_attributes();
    unordered_map<string, std::tuple<type, unsigned int, unsigned int>> rat = _right->get_attributes();
    unordered_map<string, std::tuple<type, unsigned int, unsigned int>>::const_iterator got;
    bool l_ord, r_ord = false;
    int result;
    //pega os atributos com o mesmo nome para a juncao
    for(auto &at : lat) {
        got = rat.find(at.first);
        if(got != lat.end()) {
            j_attr.push_back(at.first);
        }
    }
    //verifica se as tabelas estão ordenadas por estes atributos
    //oq fazer no caso de ter 2 atributos em comum?
    for(auto &ja : j_attr) {
        if( ja == _left->ordered_by())
            l_ord = true;
        if( ja == _right->ordered_by())
            r_ord = true;
    }
    //custoMJ
    result = _left->block_quantity() + _right->block_quantity();
    // custo das ordenacoes
    if(!l_ord) {
        int itres = (int)(log(_left->block_quantity()/ _nBuf) / log(_nBuf));
        result += 2 * _left->block_quantity() * (itres + 1);
    }
    if(!r_ord) {
        int itres = (int)(log(_right->block_quantity()/ _nBuf) / log(_nBuf));
        result += 2 * _right->block_quantity() * (itres + 1);
    }
    return result;
}
//aplicada se existir um indice hash com a mesma funcao definido para os atributos de juncao das relacoes R e S
int NaturalJoinNode::A4() const
{
    return 0;
}

int Table::size() const
{
	int res = 0;
	for(std::pair<string, std::tuple<type, int, int>> i : _attributes) {
		//gets size
        res += std::get<1>(i.second);
	}
	return res;
}
//slide4 - pagina 5
// primario arvore-B para atributo chave(caso a3 da seleção) = hIs + 1
// indice primario arvore-B para atributo nao-chave(caso a4 da seleção) = hIs + teto(Cs(ai)/fs)
// indice secundario arvore-B para atributo nao-chave(caso a6 seleção) = HIs + 1 + teto(Cs(ai))
// indice hash = 1?
unsigned int Table::primary_index_access_cost(string attribute_name) const
{

	return 0; //TODO
}

SelectionNode::SelectionNode(const Table* child, const Expression* expr) : Table("Selection(" + child->name() + ")", 0), _child(child), _expression(expr)
{}

SelectionNode::~SelectionNode(){}

int SelectionNode::tuple_quantity() const
{
	return _expression->tuple_quantity(this);
}

int SelectionNode::best_access_cost() const
{
	int bR;

	int res = A1();
	string best = "A1";

	bR = res;

	// int a2 = A2(bR);
	// if(a2 != 0 && a2 < res) {
	// 	res = a2;
	// 	best = "A2";
	// }
	return 0;
}

/*
 * A1 = bR = Pesquisa linear
 * fR = piso[tbloco/tR] fator de bloco table
 * bR = teto[nR/fR] = Numero de blocos table
 * tbloco = tamanho do bloco BD
 * tR = tamanho da tupla table
 * nR = numero tuplas table
 *
 * @return bR
*/
int SelectionNode::A1() const
{
	int fR = _block_size / _child->size();
	int bR = ceil(float(_child->tuple_quantity() / fR));

	return bR;
}

/*
 * A2 = teto[log2 bR] + teto[Cr(ai)/fR] -1 ou teto[log2 bR] se ai eh chave
 * Cr(ai) = nR/VR(ai)
 * fR = piso[tbloco/tR] fator de bloco table
 *
 *
 *
*/
// int SelectionNode::A2(int bR)
// {
// 	// qual a diferenca entre E e OU entre as expressoes, sempre considera a com menor custo pro resultado final?
// 	// data <= 'valor' ^ data >= 'valor'
// 	// (Expr ^ Expr) Expr
// 	//for(exp:Expression)
// 	//
//
//     return 0;
// }

ProductNode::ProductNode(const Table* left, const Table* right) : Table("Product" + left->name() + right->name(), left->tuple_quantity() * right->tuple_quantity()), _left(left), _right(right)
{
    //adiciona os atributos dos 2 filhos na tabela do produto com seus nomes modificados tablename+attributename
    unordered_map<string, std::tuple<type, unsigned int, unsigned int>> lat = left->get_attributes();
    unordered_map<string, std::tuple<type, unsigned int, unsigned int>> rat = right->get_attributes();
    std::tuple<type, unsigned int, unsigned int> atf;
    for(auto &a : lat){
        atf = a.second;
        add_attribute(left->name() + a.first, std::get<0>(atf), std::get<1>(atf), std::get<2>(atf));
    }
    for(auto &a : rat){
        atf = a.second;
        add_attribute(right->name() + a.first, std::get<0>(atf), std::get<1>(atf), std::get<2>(atf));
    }
}

ProductNode::~ProductNode(){}

int ProductNode::best_access_cost() const
{
    int mult = _left->block_quantity() * _right->block_quantity();
	int res = std::min<int>(_left->block_quantity() + mult, _right->block_quantity() + mult);
	return res;
}

ProjectionNode::ProjectionNode(const Table* child, deque<std::pair<string, string>> attributes) : Table("Projection" + child->name(), 0), _child(child), _attribs(attributes)
{
    //procura os atributos da projecao na tabela do filho e adiciona na tabela da projecao
    unordered_map<string, std::tuple<type, unsigned int, unsigned int>> at = child->get_attributes();
    unordered_map<string, std::tuple<type, unsigned int, unsigned int>>::const_iterator got;
    std::tuple<type, unsigned int, unsigned int> atf;
    for(auto &a : _attribs) {
        got = at.find(a.first+a.second);
        if ( got != at.end()) {
            atf = got->second;
            add_attribute(got->first, std::get<0>(atf), std::get<1>(atf), std::get<2>(atf));
        }
    }
}
ProjectionNode::~ProjectionNode(){}

int ProjectionNode::best_access_cost() const
{
    return _child->block_quantity();
}

JoinNode::JoinNode(const Table* left, const Table* right, const Expression* expression) : Table("Join" + left->name() + right->name(), left->tuple_quantity() * right->tuple_quantity()), _left(left), _right(right), _expression(expression)
{

}

JoinNode::~JoinNode(){}

int JoinNode::best_access_cost() const
{
	//we need to log all these results
	int res = A1();
	string best = "A1";
	int a2 = A2();
	if(a2 != 0 && a2 < res) {
		res = a2;
		best = "A2";
	}
	int a3 = A3();
	if(a3 != 0 && a3 < res) {
		res = a3;
		best = "A3";
	}

	int a4 = A4();
	if(a4 != 0 && a4 < res) {
		res = a4;
		best = "A4";
	}
	return res;
}

int JoinNode::A1() const
{
	//is always possible
	int mult = _left->block_quantity() * _right->block_quantity();
	int res = std::min<int>(_left->block_quantity() + mult, _right->block_quantity() + mult);
	return res;
}

int JoinNode::A2() const
{
    return 0;
}

int JoinNode::A3() const
{
    return 0;
}

int JoinNode::A4() const
{
    return 0;
}


/*
class SelectionNode : public Table {

	public:

		int best_access_cost();

	private:
		//tree containing the expression
		Expression _expression;

};

class JoinNode : public Table {

	public:
		JoinNode(const Table* left, const Table* right, Expression expression);
		virtual ~JoinNode();

		int best_access_cost();

	private:
		Expression _expression;

};

class NaturalJoinNode : public Table {

	public:


};

class Database {

	public:
		Database(int nbuf, int block_size);
		void insert_table(Table table);

	private:
		//buffer size
		int _nBuf;
		//disk block size in bytes
		int _block_size;
		deque<Table> _tables;
};
*/
