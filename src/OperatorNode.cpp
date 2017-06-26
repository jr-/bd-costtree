#include "OperatorNode.hpp"

#include <algorithm>
#include <cmath>
#include <iostream>
#include <exception>

using std::string;
using std::deque;
using std::unordered_map;
using std::pair;

Table::Table() {}

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


int Table::tuple_quantity() const
{
	return _tuple_quantity;
}

void Table::add_attribute(string attribute_name, type attribute_type, unsigned int size, unsigned int variability)
{
	//name must be unique
	auto got = _attributes.find(attribute_name);
	if(got != _attributes.end()) {
		//attribute already exists, cant be inserted
		throw std::runtime_error("Attribute " + attribute_name + " already exists in " + _name);
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
		throw std::runtime_error(attribute_name + " is not an attribute of " + _name);
		return;
	}
	_foreign_keys.insert(std::pair<string, string>(attribute_name, foreign_table_name));
}

void Table::add_secondary_index(string attribute_name, unsigned int n, unsigned int fi)
{
	//attribute_name must be the name of an attribute of this table.
	if(_attributes.find(attribute_name) == _attributes.end()) {
		//attribute_name is not an attribute of this table
		throw std::runtime_error(attribute_name + " is not an attribute of " + _name);
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
		throw std::runtime_error(attribute_name + " is not an attribute of " + _name);
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

pair<unsigned int, unsigned int> Table::secondary_index(string attribute_name) const
{
	if(_secondary_indexes.find(attribute_name) == _secondary_indexes.end()) {
		return pair<unsigned int, unsigned int>(0, 0);
	}
	return _secondary_indexes.at(attribute_name);
}

void Table::ordered_by(string attribute)
{
	//attribute must be an attribute of this table
	if(_attributes.find(attribute) == _attributes.end()) {
		//attribute_name is not an attribute of this table
		throw std::runtime_error(attribute + " is not an attribute of " + _name);
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
	int result = ceil(table->tuple_quantity() * (1 - (1 - _left->tuple_quantity(table)) * (1 - _right->tuple_quantity(table))));
	return result;
}

int OrExpression::best_access_cost(const Table * table) const
{
	return _left->best_access_cost(table) + _right->best_access_cost(table);
}

EqualExpression::EqualExpression(const std::pair<string, string> left, const std::pair<string, string> right):  FinalExpression(left, right) {}

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
	return FinalExpression::best_access_cost(table);
}

int FinalExpression::best_access_cost(const Table * table) const
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
	if(table->primary_index(attribute) != pair<unsigned int, unsigned int>(0,0)) {//A3 and A4
		if(deque<string>{attribute} == table->primary_key()) { //A3
			int calc_a3 = table->primary_index_access_cost(attribute) + 1;
			auto a3 = pair<int, string>(calc_a3, "A3");
			results.push_back(a3);
		} else { //A4
			int calc_a4 = table->primary_index_access_cost(attribute) + ceil(table->attribute_cardinality(attribute)/table->block_factor());
			auto a4 = pair<int, string>(calc_a4, "A4");
			results.push_back(a4);
		}
	}
	if(table->secondary_index(attribute) != pair<unsigned int, unsigned int>(0,0)) { //A5 and A6
		if(deque<string>{attribute} == table->primary_key()) { //A5
			int calc_a5 = table->secondary_index_access_cost(attribute) + 1;
			auto a5 = pair<int, string>(calc_a5, "A5");
			results.push_back(a5);
		} else { //A6
			int calc_a6 = table->secondary_index_access_cost(attribute) + 1;
			calc_a6 += ceil(table->attribute_cardinality(attribute));
			auto a6 = pair<int, string>(calc_a6, "A6");
			results.push_back(a6);
		}
	}
	auto best = results.back();
	results.pop_back();
	for(auto i: results) {
		if(i.first < best.first) {
			best = i;
		}
	}
	return best.first;
}

NotEqualExpression::NotEqualExpression(const std::pair<string, string> left, const std::pair<string, string> right) : FinalExpression(left, right) {}

int NotEqualExpression::tuple_quantity(const Table* table) const
{
	int quantity;
	if(_left_attribute.first == "") {
		quantity = table->attribute_cardinality(_right_attribute.second);
	} else {
		quantity = table->attribute_cardinality(_left_attribute.second);
	}
	return table->tuple_quantity() - table->tuple_quantity() / quantity;
}

int NotEqualExpression::best_access_cost(const Table * table) const
{
	return FinalExpression::best_access_cost(table);
}

GreaterExpression::GreaterExpression(const std::pair<string, string> left, const std::pair<string, string> right) : FinalExpression(left, right) {}

int GreaterExpression::tuple_quantity(const Table* table) const
{
	return table->tuple_quantity() / 2;
}

int GreaterExpression::best_access_cost(const Table * table) const
{
	deque<pair<int, string>> results;
	pair<int, string> a1 = pair<int, string>(table->block_quantity(), "A1");
	results.push_back(a1);
	string attribute = _left_attribute.second;
	if(attribute == table->ordered_by()) { //A2
		int calc_a2 = ceil(log2(table->block_quantity()));
		if(deque<string>{attribute} != table->primary_key()) {
			calc_a2 += ceil(table->attribute_cardinality(attribute)/table->block_factor()) - 1;
		}
		calc_a2 += ceil(((double) table->block_quantity()) / 2);
		auto a2 = pair<int, string>(calc_a2, "A2");
		results.push_back(a2);
	}
	if(table->primary_index(attribute) != pair<unsigned int, unsigned int>(0,0)) {//A7
		int calc_a7 = table->primary_index_access_cost(attribute);
		calc_a7 += ceil(((double) table->block_quantity()) / 2);
		auto a7 = pair<int, string>(calc_a7, "A7");
		results.push_back(a7);
	}

	auto best = results.back();
	results.pop_back();
	for(auto i: results) {
		if(i.first < best.first) {
			best = i;
		}
	}
	return best.first;
}

LessExpression::LessExpression(const std::pair<string, string> left, const std::pair<string, string> right) : FinalExpression(left, right) {}

int LessExpression::tuple_quantity(const Table* table) const
{
	return table->tuple_quantity() / 2;
}

int LessExpression::best_access_cost(const Table * table) const
{
	deque<pair<int, string>> results;
	pair<int, string> a1 = pair<int, string>(table->block_quantity(), "A1");
	results.push_back(a1);
	string attribute = _left_attribute.second;
	if(attribute == table->ordered_by()) { //A2
		int calc_a2 = ceil(log2(table->block_quantity()));
		if(deque<string>{attribute} != table->primary_key()) {
			calc_a2 += ceil(table->attribute_cardinality(attribute)/table->block_factor()) - 1;
		}
		calc_a2 += ceil(((double) table->block_quantity()) / 2);
		auto a2 = pair<int, string>(calc_a2, "A2");
		results.push_back(a2);
	}
	if(table->primary_index(attribute) != pair<unsigned int, unsigned int>(0,0)) {//A8
		int calc_a8 = ceil(((double) table->block_quantity()) / 2);
		auto a8 = pair<int, string>(calc_a8, "A8");
		results.push_back(a8);
	}

	auto best = results.back();
	results.pop_back();
	for(auto i: results) {
		if(i.first < best.first) {
			best = i;
		}
	}
	return best.first;
}

NaturalJoinNode::NaturalJoinNode() : Table() {}

NaturalJoinNode::NaturalJoinNode(Table* left, Table* right) : Table("NaturalJoin" + left->name() + right->name(), 0), _left(left), _right(right)
{
    update();
}

NaturalJoinNode::~NaturalJoinNode(){}

void NaturalJoinNode::update(){
    if(_left != nullptr && _right != nullptr) {
        _name = "NaturalJoin" + _left->name() + _right->name();
        _tuple_quantity = 0;
        //lista de atributos com o mesmo nome
        deque<string> j_attr;
        unordered_map<string, std::tuple<type, unsigned int, unsigned int>> lat = _left->get_attributes();
        unordered_map<string, std::tuple<type, unsigned int, unsigned int>> rat = _right->get_attributes();
        unordered_map<string, std::tuple<type, unsigned int, unsigned int>>::const_iterator got;

        //pega os atributos com o mesmo nome para a juncao && add right attributes sem pegar os duplicados
        std::tuple<type, unsigned int, unsigned int> atf;
        for(auto &at : rat) {
            got = lat.find(at.first);
            if(got != lat.end()) {
                j_attr.push_back(at.first);
            } else {
                atf = at.second;
                add_attribute(_right->name() + "." + at.first, std::get<0>(atf), std::get<1>(atf), std::get<2>(atf));
            }
        }

        //add attributes com os duplicados
        for(auto &a : lat){
            atf = a.second;
            add_attribute(_left->name() + "." + a.first, std::get<0>(atf), std::get<1>(atf), std::get<2>(atf));
        }
        //calcular tuple_quantity
        if(j_attr.size() != 0){
            //considerando que so tem 1 atributo em comum
            string c_at = j_attr.at(0);
            //2 - juncao por referencia fk(R) = pk(S)
            unordered_map<string, string> fks = _left->get_fks();
            auto gotf = fks.find(c_at);
            if(gotf != fks.end()) {
                _tuple_quantity = _left->tuple_quantity();
                std::cout << _tuple_quantity << std::endl;
            }
            fks = _right->get_fks();
            gotf = fks.find(c_at);
            if(gotf != fks.end()) {
                _tuple_quantity = _right->tuple_quantity();
            }

            if(_tuple_quantity == 0) {
                bool l_unique, r_unique = false;

                double l_card = _left->attribute_cardinality(c_at);
                double r_card = _right->attribute_cardinality(c_at);
                if(l_card == 1.0){
                    l_unique = true;
                }
                if(r_card == 1.0){
                    r_unique = true;
                }
                //3 - juncao entre chaves candidatas (atributos unique) ie cpf nas duas tabelas
                if(l_unique && r_unique)
                    _tuple_quantity = std::min<int>(_left->tuple_quantity(), _right->tuple_quantity());

                if(_tuple_quantity == 0) {
                    //4 - juncao por igualdade (atributos nao chave)
                    _tuple_quantity = std::min<int>(_left->tuple_quantity()*l_card, _right->tuple_quantity()*r_card);
                }
            }
        } else {//1 - juncao natural sem atributo em comum nr * ns
            _tuple_quantity = _left->tuple_quantity() * _right->tuple_quantity();
        }
    }
}

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
//slide4 - pagina 5
// primario arvore-B para atributo chave(caso a3 da seleção) = hIs + 1
// indice primario arvore-B para atributo nao-chave(caso a4 da seleção) = hIs + teto(Cs(ai)/fs)
// indice secundario arvore-B para atributo nao-chave(caso a6 seleção) = HIs + 1 + teto(Cs(ai))
// indice hash = 1?
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
        double itres = (log(_left->block_quantity()/ _nBuf) / log(_nBuf));
        result += 2 * _left->block_quantity() * (itres + 1);
    }
    if(!r_ord) {
        double itres = (log(_right->block_quantity()/ _nBuf) / log(_nBuf));
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
	if(_primary_indexes.find(attribute_name) == _primary_indexes.end()) {
		throw std::runtime_error("Attribute " + attribute_name + " does not have a primary index in " + _name);
	}
	if(_primary_indexes.at(attribute_name) == pair<unsigned int, unsigned int>(0,1)) { //hash
		return 1;
	}
	auto index = _primary_indexes.at(attribute_name);
	//variability * N
	int inside = ceil(std::get<2>(_attributes.at(attribute_name)) / (double) index.first);
	return ceil(log(inside) / log(index.second)); // log base=fi
}

unsigned int Table::secondary_index_access_cost(string attribute_name) const
{
	if(_primary_indexes.find(attribute_name) == _primary_indexes.end()) {
		throw std::runtime_error("Attribute " + attribute_name + " does not have a secondary index in " + _name);
	}
	if(_secondary_indexes.at(attribute_name) == pair<unsigned int, unsigned int>(0,1)) { //hash
		return 1;
	}
	auto index = _secondary_indexes.at(attribute_name);
	//variability * N
	int inside = ceil(std::get<2>(_attributes.at(attribute_name)) / (double) index.first);
	return ceil(log(inside) / log(index.second)); // log base=fi
}

SelectionNode::SelectionNode(const Expression* expr) : Table(), _expression(expr) {}

SelectionNode::SelectionNode(string attribute, string literal, int expressionType)
{
	pair<string, string> left = pair<string, string>("Table", attribute);
	auto right = pair<string, string>("", literal);
	switch(expressionType) {
		case 0: _expression = new EqualExpression(left, right); break;
		case 1: _expression = new NotEqualExpression(left, right); break;
		case 2: _expression = new GreaterExpression(left, right); break;
		case 3: _expression = new LessExpression(left, right); break;
	}
}

SelectionNode::SelectionNode(Table* child, const Expression* expr) : Table("Selection(" + child->name() + ")", 0), _child(child), _expression(expr)
{}

SelectionNode::~SelectionNode(){}

void SelectionNode::update()
{
    if(_child == nullptr) {
		return;
	}
    _name = "Selection(" + _child->name() + ")";
	_attributes = _child->get_attributes();
	_primary_key = _child->primary_key();
	_foreign_keys = _child->get_fks();
}

int SelectionNode::tuple_quantity() const
{
	return _expression->tuple_quantity(_child);
}

int SelectionNode::best_access_cost() const
{
	return _expression->best_access_cost(_child);
}

ProductNode::ProductNode() : Table() {}

ProductNode::ProductNode(Table* left, Table* right) : Table("Product" + left->name() + right->name(), left->tuple_quantity() * right->tuple_quantity()), _left(left), _right(right)
{

}

ProductNode::~ProductNode(){}

void ProductNode::update()
{
    if(_left != nullptr && _right != nullptr) {
        _name = "Product" + _left->name() + _right->name();
        _tuple_quantity = _left->tuple_quantity() * _right->tuple_quantity();
        //adiciona os atributos dos 2 filhos na tabela do produto com seus nomes modificados tablename+attributename
        unordered_map<string, std::tuple<type, unsigned int, unsigned int>> lat = _left->get_attributes();
        unordered_map<string, std::tuple<type, unsigned int, unsigned int>> rat = _right->get_attributes();
        std::tuple<type, unsigned int, unsigned int> atf;
        for(auto &a : lat){
            atf = a.second;
            add_attribute(_left->name() + a.first, std::get<0>(atf), std::get<1>(atf), std::get<2>(atf));
        }
        for(auto &a : rat){
            atf = a.second;
            add_attribute(_right->name() + a.first, std::get<0>(atf), std::get<1>(atf), std::get<2>(atf));
        }
    }
}

int ProductNode::best_access_cost() const
{
    int mult = _left->block_quantity() * _right->block_quantity();
	int res = std::min<int>(_left->block_quantity() + mult, _right->block_quantity() + mult);
	return res;
}

ProjectionNode::ProjectionNode(deque<std::pair<string, string>> attributes) : Table(), _attribs(attributes) {}

ProjectionNode::ProjectionNode(Table* child, deque<std::pair<string, string>> attributes) : Table("Projection" + child->name(), child->tuple_quantity()), _child(child), _attribs(attributes)
{
    update();
}
ProjectionNode::~ProjectionNode(){}

void ProjectionNode::update()
{
    if (_child != nullptr) {
        _name = "Projection" + _child->name();
        _tuple_quantity = _child->tuple_quantity();
        //procura os atributos da projecao na tabela do filho e adiciona na tabela da projecao
        unordered_map<string, std::tuple<type, unsigned int, unsigned int>> at = _child->get_attributes();
        unordered_map<string, std::tuple<type, unsigned int, unsigned int>>::const_iterator got;
        std::tuple<type, unsigned int, unsigned int> atf;
        for(auto &a : _attribs) {
            got = at.find(a.second);
            if ( got != at.end()) {
                atf = got->second;
                add_attribute(a.first + "." + a.second, std::get<0>(atf), std::get<1>(atf), std::get<2>(atf));
            }
        }
    }
}

int ProjectionNode::best_access_cost() const
{
    return _child->block_quantity();
}

JoinNode::JoinNode(const JoinExpression* expression) : Table(), _expression(expression) {}

JoinNode::JoinNode(Table* left, Table* right, const JoinExpression* expression) : Table("Join" + left->name() + right->name(), left->tuple_quantity() * right->tuple_quantity()), _left(left), _right(right), _expression(expression)
{

}

JoinNode::~JoinNode(){}

void JoinNode::update()
{
    if(_left != nullptr && _right != nullptr) {
        _name = "Join" + _left->name() + _right->name();
        _tuple_quantity = 0;
        //TODO Populate attributes and calc tuple_quantity

        std::pair<string, string> left_at_name = _expression->left_at();
        std::pair<string, string> right_at_name = _expression->right_at();
        JoinExpression::JoinExpressionOperator operator_type = _expression->operator_type();

        unordered_map<string, std::tuple<type, unsigned int, unsigned int>> lat = _left->get_attributes();
        unordered_map<string, std::tuple<type, unsigned int, unsigned int>> rat = _right->get_attributes();

        //pega os atributos com o mesmo nome para a juncao && add right attributes sem pegar os duplicados
        std::tuple<type, unsigned int, unsigned int> atf;
        for(auto &at : rat) {
            if(at.first != right_at_name.second) {
                atf = at.second;
                add_attribute(_right->name() + "." + at.first, std::get<0>(atf), std::get<1>(atf), std::get<2>(atf));
            }
        }

        //add attributes com os duplicados
        for(auto &a : lat){
            atf = a.second;
            add_attribute(_left->name() + "." + a.first, std::get<0>(atf), std::get<1>(atf), std::get<2>(atf));
        }

        //calcular tuple_quantity
        //considerando que so tem 1 expressao
        if(operator_type == JoinExpression::JoinExpressionOperator::Equal) {
            //2 - juncao por referencia fk(R) = pk(S)
            unordered_map<string, string> fks = _left->get_fks();
            auto gotf = fks.find(left_at_name.second);
            if(gotf != fks.end()) {
                _tuple_quantity = _left->tuple_quantity();
                std::cout << _tuple_quantity << std::endl;
            }
            fks = _right->get_fks();
            gotf = fks.find(right_at_name.second);
            if(gotf != fks.end()) {
                _tuple_quantity = _right->tuple_quantity();
            }

            if(_tuple_quantity == 0) {
                bool l_unique, r_unique = false;

                double l_card = _left->attribute_cardinality(left_at_name.second);
                double r_card = _right->attribute_cardinality(right_at_name.second);
                if(l_card == 1.0){
                    l_unique = true;
                }
                if(r_card == 1.0){
                    r_unique = true;
                }
                //3 - juncao entre chaves candidatas (atributos unique) ie cpf nas duas tabelas
                if(l_unique && r_unique)
                    _tuple_quantity = std::min<int>(_left->tuple_quantity(), _right->tuple_quantity());

                if(_tuple_quantity == 0) {
                    //4 - juncao por igualdade (atributos nao chave)
                    _tuple_quantity = std::min<int>(_left->tuple_quantity()*l_card, _right->tuple_quantity()*r_card);
                }
            }
        } else {// 5 - juncao por desigualdade
            _tuple_quantity = (_left->tuple_quantity()*(_right->tuple_quantity()/2) + _right->tuple_quantity()*(_left->tuple_quantity()/2))/2;
        }
    }
}

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
    std::pair<string, string> left_at_name = _expression->left_at();
    std::pair<string, string> right_at_name = _expression->right_at();
    JoinExpression::JoinExpressionOperator operator_type = _expression->operator_type();
    bool l_ord, r_ord = false;
    int result = 0;
    if( left_at_name.second == _left->ordered_by())
        l_ord = true;
    if( right_at_name.second == _right->ordered_by())
        r_ord = true;
    //custoMJ
    result = _left->block_quantity() + _right->block_quantity();
    // custo das ordenacoes
    if(!l_ord) {
        double itres = (log(_left->block_quantity()/ _nBuf) / log(_nBuf));
        result += 2 * _left->block_quantity() * (itres + 1);
    }
    if(!r_ord) {
        double itres = (log(_right->block_quantity()/ _nBuf) / log(_nBuf));
        result += 2 * _right->block_quantity() * (itres + 1);
    }
    return result;
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
