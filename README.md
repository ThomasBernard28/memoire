# Mémoire

**Titre:**
Étude empirique des GitHubs Actions sur les plateformes de développement collaboratif

**Auteur:** Thomas BERNARD

**Co-directeurs:** Alexandre DECAN, Tom MENS

**Abstract:**

Ce mémoire présente une étude empirique et temporelle de l'utilisation des des GitHub Actions, un outil d'intégration et de déploiement continu (CI/CD), au sein des plateformes de développement collaboratif. Deux axes principaux structurent cette recherche. Le premier a pour but d'examiner la compatibilité et l'adoption des GitHub Actions au sein de plateformes de développement collaboratif autres que GitHub et plus précisément Gitea. L'analyse met en évidence des limitations notables concernant cette dernière : compatibilité restreinte des workflows GitHub Actions, nécessité d'auto-hébergement des runners, et faible taux d'adoptions des Gitea Actions (système maison pour la compatibilité avec GitHub Actions) par les utilisateurs de la plateforme avec seulement 4,5\% des dépôts qui utilisent ce système soit 755 dépôts au total. ces obstacles rendent difficile toute comparaison empirique rigoureuse avec GitHub en l'absence d'un jeu de données de taille suffisante.

Face à cela, le second axe se focalise sur une étude comparative et temporelle de l'usage des GitHub Actions au sein de la plateforme GitHub en elle même. À partir d'un jeu de données récemment publié par Cardoen (2025) [2] couvrant plus de 40\,000 dépôts et pas loin de 2\,000\,000 d'itérations de fichiers de workflows allant de 2019 à octobre 2024, ce mémoire réplique d'une part une étude empirique menée en 2022 par Decan et al. [5] sur base des données couvrant l'année 2022 au sein du jeu de donnée mentionné. D'autre part, le jeu de données est exploité afin de créer une étude temporelle ayant pour but d'analyser l'évolution des pratiques, l'utilisation des différentes fonctionnalités (types de steps, événements déclencheurs, Actions les plus utilisées, etc.) au fil des années. Les résultats pour la partie comparative montre de légère variations probablement introduites par la différence de taille et de critères de sélection entre les deux jeux de données. En ce qui concerne les résultats de l'analyse temporelle, ceux-ci montrent une adoption croissante et diversifiée des GitHub Actions au fil des années, avec des tendances spécifiques sur les types d'Actions, l'usage des permissions ou encore de la \emph{matrix strategy}. 

Ce travail contribue à une meilleure compréhension de l'intégration des outils CI/CD dans les écosystèmes collaboratifs modernes et permet de visualiser l'évolution de cette intégration. Ce travail ouvre également la voie à de futures recherches sur l'adoption de GitHub Actions ou de solutions similaires sur plusieurs plateformes. 
