# Q 10
# calculate the variable x_diff
x_diff =  31.848 - 29.319
x_diff

se = (10.3808 - 11.9676)/sqrt(254)
x_diff/se
# NO ME SALIO


## Q 18
# Constructing the matrix
grad_gender <- matrix(c(111, 186, 76, 71), ncol=2)
colnames(grad_gender) <- c('W','M')
rownames(grad_gender) <- c('Grad','NotG')


# Call the prop.test function on the matrix vote_behavior
prop.test(grad_gender, conf.level = 0.95, correct = FALSE)

# calculate the difference in sample proportions and store it in a variable called difference
pwoman = 0.3737373737 
pmen = 0.5170068027
difference <- pwoman - pmen

# calculate the pooled estimate and store it in a variable called pooled
pooled <- ((pwoman * 297) + (pmen * 147)) / (297 + 147)
pooled
# calculate the standard error and store it in a variable called se
se <- sqrt(pooled * (1 - pooled) * ((1 / 297) + (1/ 147)))
se

# calculate the z value and store it in a variable called z_value
  z_value = difference /se
z_value

p_value = pnorm(z_value, lower.tail = F)
p_value

# 27

# calculate the difference in sample proportions and store it in a variable called difference
Lgrad = 0.4951456311
SCgrad = 0.6338461538
difference <- Lgrad - SCgrad
difference

# calculate the pooled estimate and store it in a variable called pooled
pooled <- ((Lgrad * 103 ) + (SCgrad * 325 )) / ( 103 + 325 )
pooled
# calculate the standard error and store it in a variable called se
se <- sqrt(pooled * (1 - pooled) * ((1 / 103) + (1/ 325)))
se
# calculate the z value and store it in a variable called Z_value
z_value = difference / se
z_value

# 27 otra versión
# Constructing the matrix
vote_behavior <- matrix(c(51, 206, 52, 119), ncol=2)
colnames(vote_behavior) <- c('left','no left')
rownames(vote_behavior) <- c('male','female')

# Call the prop.test function on the matrix vote_behavior
prop.test(vote_behavior, conf.level = 0.95, correct = FALSE)


# 29

# average difference between male and female sample in hours of sport per week
mean_difference <- 6.1078 - 5.1313

# standard error of the difference between male and female sample in hours of sport per week
se <- sqrt((1.51213^2 / 12) + (1.26706^2 / 13))

# calculate the t score and assign it to the variable t_score
t_score = (6.1078 - 5.1313 -0)/ se
t_score

# calculate the degrees of freedom and store it in a variable called df
df = 12+13-2

# calculate the p value
pt(t_score, df)*2

upper_value = qt(0.95, df)*se + mean_difference
upper_value
se




