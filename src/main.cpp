#include <string>
#include <iostream>

#include "OperatorNode.hpp"
using namespace std;

//valores da lista de exercicios2
int _block_size = 1024;
int _nBuf = 5;

int main() {
	Table consultas("Consultas", 1000);
	consultas.add_attribute("codm", INT, 5, 80);
	consultas.add_attribute("codp", INT, 10, 450);
	consultas.add_attribute("data", DATE, 10, 300);
	consultas.add_attribute("hora", INT, 5, 15);

	consultas.add_secondary_index("codm", 5, 5);
	consultas.add_secondary_index("codp", 5, 5);
	consultas.add_secondary_index("data", 5, 5);
	consultas.add_primary_index("data", 5, 5);
	consultas.add_secondary_hash_index("codm");
	consultas.add_secondary_hash_index("codp");

	consultas.ordered_by("data");
	consultas.add_foreign_key("codm", "Medicos");
	consultas.add_foreign_key("codp", "Pacientes");
	//Pacientes will not be used in this example, but will be put for completion sake
	Table medicos("Medicos", 100);
	medicos.add_attribute("codm", INT, 5, 100);
	medicos.add_attribute("nome", STRING, 15, 100);
	medicos.add_attribute("cidade", STRING, 15, 50);
	medicos.add_attribute("idade", INT, 5, 40);
	medicos.add_attribute("especialidade", STRING, 10, 10);

	medicos.add_primary_key(deque<string>{"codm"});
	medicos.add_primary_index("codm", 10, 10);
	medicos.add_secondary_hash_index("especialidade");
	medicos.add_secondary_index("cidade", 5, 5);

	medicos.ordered_by("codm");

	EqualExpression selectionLeftExpression(std::pair<string, string>("Consultas", "data"), std::pair<string, string>("", "15/10/2007"));
	SelectionNode selection_left(&selectionLeftExpression);
	selection_left.set_child(&consultas);
	ProjectionNode projection_left(deque<std::pair<string, string>>{std::pair<string, string>("Consultas", "codm")});
	projection_left.set_child(&selection_left);

	NotEqualExpression not_equal(pair<string, string>("Medicos", "cidade"), pair<string, string>("", "Florianopolis"));
	EqualExpression equal(pair<string, string>("Medicos", "especialidade"), pair<string, string>("", "ortopedista"));
	AndExpression selectionRightExpression(&equal, &not_equal);
	SelectionNode selection_right(&selectionRightExpression);
    selection_right.set_child(&medicos);
	ProjectionNode projection_right(deque<pair<string, string>>{pair<string, string>("Medicos", "codm"), pair<string, string>("Medicos", "nome")});
    projection_right.set_child(&selection_right);

	NaturalJoinNode natural_join;
    natural_join.set_child_left(&projection_left);
    natural_join.set_child_right(&projection_right);

	cout << "Custo da seleção da direita: " << to_string(selection_right.best_access_cost()) << endl;
	cout << "Quantidade de tuplas da seleção da direita: " << to_string(selection_right.tuple_quantity()) << endl;
	cout << "Tamanho total da seleção da direita: " << to_string(selection_right.total_table_size()) << endl;

	cout << "Custo da projeção da direita: " << to_string(projection_right.best_access_cost()) << endl;
	cout << "Tamanho total da projeção da direita: " << to_string(projection_right.total_table_size()) << endl;;

	cout << "Custo da seleção da esquerda: " << to_string(selection_left.best_access_cost()) << endl; //A4
	cout << "Quantidade de tuplas da seleção da esquerda: " << to_string(selection_left.tuple_quantity()) << endl;
	cout << "Tamanho total da seleção da esquerda:" << to_string(selection_left.total_table_size()) << endl;

	cout << "Custo da projeção da esquerda: " << to_string(projection_left.best_access_cost()) << endl;
	cout << "Tamanho total da projeção da esquerda: " << to_string(projection_left.total_table_size()) << endl;

	cout << "Custo do acesso do join natural: " << to_string(natural_join.best_access_cost()) << endl;;
	cout << "Quantidade de tuplas do join natural: " << to_string(natural_join.tuple_quantity()) << endl;
	cout << "Tamanho total do join natural: " << to_string(natural_join.total_table_size()) << endl;

}
