#include <string>
#include <iostream>

#include "BinaryNode.hpp"
#include "OperatorNode.hpp"
using namespace std;

/*
* TODO 1 Metodo recursivo que vai até encontrar NodeType == TABLE e vai voltar calculando custo e quantidade de tuplas
* TODO 2 Classe e logica pro "Banco de Dados"
* TODO 3 Separar o insert em insertLeft e insertRight?
*/
int main() {
    BinaryNode* root = new BinaryNode("N", BinaryNode::NodeType::NATURAL);
    root->insert("codm, nome", BinaryNode::NodeType::PROJECTION);
    root->insert("codm", BinaryNode::NodeType::PROJECTION);
    root->getLeft()->insert("cid <> 'fln' ^ esp = 'ort'", BinaryNode::NodeType::SELECTION);
    root->getLeft()->getLeft()->insert("Medicos", BinaryNode::NodeType::TABLE);
    root->getRight()->insert("data = '15/10/2007'", BinaryNode::NodeType::SELECTION);
    root->getRight()->getLeft()->insert("Consultas", BinaryNode::NodeType::TABLE);
    cout << root->getLeft()->getLeft()->getLeft()->getExp() << endl; //Medicos
    cout << root->getRight()->getLeft()->getLeft()->getExp() << endl; //Consultas
}
